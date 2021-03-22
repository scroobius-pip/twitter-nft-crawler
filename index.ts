import { Queue, Worker, Job, QueueScheduler } from 'bullmq';
import * as redis from 'ioredis'




const redisClient = new redis('ec2-3-236-123-111.compute-1.amazonaws.com', { enableAutoPipelining: true })

const schedulerQueue = new QueueScheduler('scheduler')

const importQueue = new Queue('import')
const exportQueue = new Queue('export') //QUEUE OF MARSHALLED



function addToRedisSet(marshalledUsersInfo: MarshalledUserInfo[]): Promise<number> {
    return redisClient.sadd('twitterusers', marshalledUsersInfo)
}

async function getNonExistingUsersInfo(marshalledUsersInfo: MarshalledUserInfo[]): Promise<MarshalledUserInfo[]> {
    const result = await (redisClient as any).smismember('twitterusers', marshalledUsersInfo) as number[]
    return marshalledUsersInfo.filter((_, i) => result[i] === 0)
}

type UserName = string //actually user handle
type MarshalledUserInfo = string

interface UserInfo {
    name: UserName
    followers: UserName[]
    photoUrl: string
}


async function addUserImportJob(usernames: string) {
    await importQueue.add('import', usernames, { attempts: 20, backoff: { type: 'exponential', delay: 1000 } })
}

async function addUserInfoExportJob(marshalledUsersInfo: string) {
    await exportQueue.add('export', marshallUserInfo, { attempts: 50, backoff: { type: 'exponential', delay: 100 } })
}


async function addInitialJob() {
    await addUserImportJob('simdi_jinkins')
}




const importWorker = new Worker<UserName, void>('import', async (job) => {
    const userNames = job.data.split(',')
    const usersInfo = await getUsersInfo(userNames)

    const filteredMarshalledUsersInfo = usersInfo
        .filter(({ followers }) => followers.length > 200)
        .map(marshallUserInfo)


    const nonExistingUsersInfo = await getNonExistingUsersInfo(filteredMarshalledUsersInfo)
    const nonExistingUsersNames = nonExistingUsersInfo.map(getUserNameFromMarshalledUserInfo)

    const followerUserNames = usersInfo.filter(({ name }) => nonExistingUsersNames.includes(name)).flatMap(u => u.followers)

    await addUserInfoExportJob(filteredMarshalledUsersInfo.join(','))
    await addUserImportJob(followerUserNames.join(','))

}, { concurrency: 10 })

const exportWorker = new Worker<string, void>('export', async (job) => {
    await addToRedisSet(job.data.split(',')) //EXPORT USER INFO
}, { concurrency: 1000 })



async function getUsersInfo(usernames: UserName[]): Promise<UserInfo[]> {
    return []
}


function marshallUserInfo({ name, photoUrl, followers }: UserInfo): MarshalledUserInfo {
    return [name, parsePhotoIdFromPhotoUrl(photoUrl), followers.length].join('\n')
}

function getUserNameFromMarshalledUserInfo(marshalledUsersInfo: MarshalledUserInfo): string {
    return marshalledUsersInfo.split('\n')[0]
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