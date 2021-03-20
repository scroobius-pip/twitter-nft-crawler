import { Queue, Worker, Job, QueueScheduler } from 'bullmq';


const schedulerQueue = new QueueScheduler('scheduler')

const importQueue = new Queue('import');
const exportQueue = new Queue('export')


type UserName = string //actually user handle
type MarshalledUserInfo = string

interface UserInfo {
    id: string
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
    await addUserImportJob('simdi_jinkins,twitterapi')

    // await exportQueue.add('export', 'simdi_jinkins\n1195946508006412293/ZKddKyho,simdi_jinkins\n1195946508006412293/ZKddKyho,simdi_jinkins\n1195946508006412293/ZKddKyho', { removeOnFail: 1000 })

}




const importWorker = new Worker<UserName, void>('import', async (job) => {
    const userNames = job.data.split(',')

    const existingUserCount = await getExistingUserNameCount(userNames)


    const shouldContinue = existingUserCount < (userNames.length / 2)
    if (!shouldContinue) return

    const usersInfo = await getUsersInfo(userNames)


    const filteredMarshalledUserInfo = usersInfo
        .filter(({ followers }) => followers.length > 20000)
        .map(marshallUserInfo)
        .join(',')

    const followerUserNames = usersInfo
        .map(({ followers }) => followers.join(','))
        .join(',')


    await addUserInfoExportJob(filteredMarshalledUserInfo)
    await addUserImportJob(followerUserNames)

}, { concurrency: 10 })

const exportWorker = new Worker('export', async (job) => {
    console.log(job.data)
    return ''
}, { concurrency: 100 })

// importWorker.on('completed', (job) => {
//     console.log(`${job.data} completed`)
// })

// exportWorker.on('completed', (job, returnValue) => {
//     console.log(`${job.data} completed`)
// })

async function getUsersInfo(usernames: UserName[]): Promise<UserInfo[]> {
    return []
}

async function getFollowersUserName(userId: string): Promise<UserName[]> {
    return []
}

async function getExistingUserNameCount(usernames: UserName[]): Promise<number> {
    return 0
}

async function exportUserInfo(marshalledUsersInfo: MarshalledUserInfo[]): Promise<true> {

    return true

}

function marshallUserInfo({ id, name, photoUrl, followers }: UserInfo): MarshalledUserInfo {
    return [id, name, parsePhotoIdFromPhotoUrl(photoUrl), followers.length].join('\n')
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