
export default class Queue {

    private queues: { [key: string]: Worker[] } = {};

    public static instance: Queue;

    constructor(chunk_size) {
        Queue.instance = this;
        setTimeout(this.ProcessQueues, Environment.queue_chunks);
    }

    public StartQueue(queueName: string = 'queue', workerCount: number = 1) {
        if (this.QueueExists(queueName)) {
            throw new Error('Queue already exists!');
        } else {
            let workerArray = [];
            for (let workerIndex = 0; workerIndex < workerCount; workerIndex++) {
                workerArray[workerIndex] = cluster.fork({
                    application_mode: APPLICATION_QUEUE_WORKER,
                    queueName,
                });
            }

            this.queues[queueName] = workerArray;
        }
    }

    public QueueExists(queueName: string): boolean {
        return this.queues[queueName] !== undefined;
    }

    public QueueWorkerCount(queueName: string): number {
        if (this.QueueExists(queueName)) {
            return this.queues[queueName].length;
        }
        return 0;
    }

    private async ProcessQueues() {
        Redis.connection().keys(REDIS_QUEUE_CHUNKS + '*', (err, reply) => {
            if (!err) {
                let now = Date.now() + 250;
                reply.forEach(element => {
                    let key = element.substring(13);
                    let queueOffset = key.indexOf(':');

                    if (queueOffset == undefined)
                        return;

                    let queueName = key.substring(0, queueOffset);
                    if (Number.parseInt(key.substring(queueOffset + 1)) < now) {
                        Redis.connection().multi().smembers(element).del(element).exec((err, values) => {
                            Redis.connection().rpush(queueName, values[0]);
                        });
                    }
                });
            }
        });
    }


    public static Enqueue(queueName: string, job: Job, time: number = 0): Promise<boolean> {
        let now = Date.now() + 1000;
        if (time < now) {
            time = now;
        }

        return new Promise((resolve) => {
            let chunkKey = REDIS_QUEUE_CHUNKS + queueName + ':' + (time - time % Environment.queue_chunks);
            Redis.connection().sadd(chunkKey, JSON.stringify(job), (err: any, reply: number) => {
                resolve(!err);
            });
        });
    }

    public GetActiceJobs(): Promise<Dictionary<string[] | number>> {
        let queueNames = Object.keys(this.queues);
        let jobs: Dictionary<string[] | number> = {};
        return new Promise(async (resolve) => {
            for (let queueName of queueNames) {
                jobs[queueName] = await this.GetActiveJobsForQueue(queueName);
                jobs[queueName + ":worker"] = this.queues[queueName].length;
            }
            resolve(jobs);
        })
    }

    public GetActiveJobsForQueue(queueName: string): Promise<string[]> {
        return new Promise((resolve) => {
            Redis.connection().lrange(queueName, 0, 200, (err, reply) => {
                resolve(reply);
            });
        })
    }
}