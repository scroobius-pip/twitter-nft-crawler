import * as redis from 'ioredis'


// import { promisify } from 'util'

const redisClient = new redis('ec2-3-236-123-111.compute-1.amazonaws.com', { enableAutoPipelining: true })


function addToRedisSet(): Promise<number> {
    return redisClient.sadd('twitterusers', ['hello', 'hi'])
}

function getNonExistingUsersInfo(): Promise<number[]> {

    return (redisClient as any).smismember('twitterusers', ['hello', 'hey'])

}

async function start() {
    await addToRedisSet()
    const result = await getNonExistingUsersInfo()
    console.log(result)

}

start()