import { Queue, Worker, Job, QueueScheduler } from 'bullmq';
import * as redis from 'ioredis'
import * as needle from 'needle'
import chunk = require('chunk');
import { pRateLimit } from 'p-ratelimit'
import pipe = require('p-pipe')

const redisClient = new redis('ec2-3-236-123-111.compute-1.amazonaws.com', { enableAutoPipelining: true })

const queueSchedulers = [new QueueScheduler('import',), new QueueScheduler('follower_import'), new QueueScheduler('export')]

const queueOptions = { defaultJobOptions: { removeOnComplete: true } }

const importQueue = new Queue('import', queueOptions)
const followerImportQueue = new Queue('follower_import', queueOptions)
const exportQueue = new Queue('export', queueOptions) //QUEUE OF MARSHALLED


interface TwitterUser {
    "profile_image_url": string
    "username": string
    "id": string,
    "public_metrics": {
        "followers_count": number,
        "following_count": number,
        "tweet_count": number,
    }
}

const getTwitterUsersLimit = pRateLimit({
    interval: 15 * 60 * 1000,
    rate: 500
})

const getTwitterFollowersLimit = pRateLimit({
    interval: 15 * 60 * 1000,
    rate: 20
})

async function getTwitterUsers(userIds: string[]): Promise<TwitterUser[]> {

    const twitterUserResponse = await getTwitterUsersLimit(() => needle('get', `https://api.twitter.com/2/users?ids=${userIds.join(',')}&user.fields=profile_image_url,public_metrics`, {
        headers: {
            'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAABCINwEAAAAAE84d%2BfYIClOTvkrWajggz6%2FnQEo%3DCFjvHp6J0wnPIQSCA0IF9RLr0aPI4O7MkevqKsiawqJihElwmB'
        }
    }))

    if (twitterUserResponse.body.errors) throw Error(twitterUserResponse.body.errors)
    const twitterUsers = twitterUserResponse.body.data as Array<TwitterUser>

    return twitterUsers
}

async function getTwitterFollowers(userid: string): Promise<TwitterUser[]> {
    const twitterUserResponse = await getTwitterFollowersLimit(() => needle('get', `https://api.twitter.com/2/users/${userid}/following?user.fields=profile_image_url,created_at,public_metrics&max_results=1000`, {
        headers: {
            'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAABCINwEAAAAAE84d%2BfYIClOTvkrWajggz6%2FnQEo%3DCFjvHp6J0wnPIQSCA0IF9RLr0aPI4O7MkevqKsiawqJihElwmB'
        }
    }))
    if (!twitterUserResponse.body.data) throw Error(twitterUserResponse.body)
    const twitterUsers = twitterUserResponse.body.data as Array<TwitterUser>

    return twitterUsers
}


function addToRedisSet(marshalledUsersInfo: MarshalledUserInfo[], userIds: string[]): Promise<[number, number]> {
    if (!userIds.length) return

    return Promise.all([
        redisClient.sadd('twitterIds', userIds),
        redisClient.sadd('twitterusers', marshalledUsersInfo)
    ])


}

async function removeExistingUsers(twitterUsers: TwitterUser[]): Promise<TwitterUser[]> {
    const userids = twitterUsers.map(t => t.id)
    const result = await (redisClient as any).smismember('twitterIds', userids) as number[]
    return twitterUsers.filter((_, i) => result[i] === 0)
}

type MarshalledUserInfo = string



async function addFollowerImportJob(id: string) {
    await followerImportQueue.add('follower_import', id, { attempts: 20, backoff: { type: 'exponential', delay: 1000 } })
}

async function addUserImportJob(userids: string[]) {
    await importQueue.add('import', userids, { attempts: 20, backoff: { type: 'exponential', delay: 1000 } })
}

async function addUserInfoExportJob(usersInfo: TwitterUser[]) {
    await exportQueue.add('export', usersInfo, { attempts: 500, backoff: { type: 'linear', delay: 10 } })
}


async function addInitialJob() {
    // await addUserImportJob(['813286'])
    const jobs = await exportQueue.getJobs(["active", "waiting", "delayed", "complete", "completed"])
    for await (const job of jobs) {

        const state = await job.getState()
        console.log(state)
    }

}

const createUserImportJobs = (validAccounts: TwitterUser[]): TwitterUser[] => {
    const userIds = validAccounts.map(a => a.id);
    chunk(userIds, 100).forEach(addUserImportJob);
    return validAccounts;
};

const createFollowerImportJobs = (validAccounts: TwitterUser[]): TwitterUser[] => validAccounts.map(validAccount => {
    addFollowerImportJob(validAccount.id);
    return validAccount;
});

const followerImportWorker = new Worker<string, void>('follower_import', async (job) => {
    await followerImportPipeline(job.data)
}, { concurrency: 15 })


const followerImportPipeline = pipe(
    getTwitterFollowers,
    removeInvalidAccounts,
    removeExistingUsers,
    createUserImportJobs,
    addUserInfoExportJob)


followerImportWorker.on('completed', job => {
    console.log(`(follower-import) done:${job.id}`)
})

followerImportWorker.on('failed', (job: Job) => {
    console.error(`(follower-import) failed: ${job.id} ${job.data} ${JSON.stringify(job.failedReason)}`)
})


const importPipeline = pipe(
    getTwitterUsers,
    createFollowerImportJobs,
    removeInvalidAccounts,
    addUserInfoExportJob
);

const importWorker = new Worker<string[], void>('import', async (job) => {
    await importPipeline(job.data)
}, { concurrency: 300 })


importWorker.on('completed', job => {
    console.log(`(import) done: ${job.id}`)
})

importWorker.on('failed', job => {
    console.error(`(import) failed: ${job.id} ${JSON.stringify(job.data)} ${JSON.stringify(job.failedReason)}`)
})

const exportWorker = new Worker<TwitterUser[], void>('export', async (job) => {
    try {
        const marshalledUsersInfo = job.data.map(marshallUserInfo)
        const userIds = job.data.map(d => d.id)
        await addToRedisSet(marshalledUsersInfo, userIds)
    } catch (error) {
        console.error(error)
        if (error.message.includes('photo')) {
            return
        }

        throw error
    }
}, { concurrency: 1000 })



exportWorker.on('completed', job => {
    console.log(`(export) done:${job.id}`)
})

exportWorker.on('failed', ({ data, ...job }) => {
    console.error(`(export) failed: ${job.reasonFailed} ${JSON.stringify(data)}`)
})

function removeInvalidAccounts(usersInfo: TwitterUser[]) {
    return usersInfo
        .filter(({ public_metrics: { followers_count, following_count, tweet_count }, profile_image_url }) =>
            following_count >= 100 && followers_count > 5 && tweet_count > 3 && profile_image_url !== 'https://abs.twimg.com/sticky/default_profile_images/default_profile_400x400.png'
        );
}

function marshallUserInfo({ username, profile_image_url: photoUrl, public_metrics: { followers_count } }: TwitterUser): MarshalledUserInfo {
    return [username, parsePhotoIdFromPhotoUrl(photoUrl), followers_count].join('\n')
}

function parsePhotoIdFromPhotoUrl(photoUrl: string): string {
    try {
        const regex = /(?:s\/)(.*)(?:_)/
        return regex.exec(photoUrl)[1]
    } catch (error) {
        throw Error('Could not get photo id from url:' + photoUrl)
    }
}



addInitialJob()
    .then(() => { queueSchedulers.forEach(s => s.close()) })
    .catch(console.error)


    // https://abs.twimg.com/sticky/default_profile_images/default_profile_400x400.png