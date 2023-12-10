class RedisSinkPartition(StatelessSinkPartition):
    """
    Output partition that writes to a Redis instance.
    """
    def __init__(self):
        self._client = redis.Redis(host='127.0.0.1', db=11)

    def write_batch(self, items: list) -> None:
        """
        Write a batch of output items to Redis. Called multiple times whenever new items are seen at this
        point in the dataflow. Non-deterministically batched.
        """
        with self._client.pipeline() as pipe:
            for key, value in items:
                pipe.set(key, value)
            pipe.execute()

    def close(self) -> None:
        """
        Clean up this partition when the dataflow completes. This is not guaranteed to be called on unbounded data.
        """
        self._client.close()


class RedisSink(DynamicSink):
    """
    An output sink where all workers write items to a Redis instance concurrently.

    Does not support storing any resume state. Thus, these kinds of
    outputs only naively can support at-least-once processing.
    """
    def build(self, worker_index: int, worker_count: int) -> RedisSinkPartition:
        """
        Build an output partition for a worker. Will be called once on each worker.
        """
        return RedisSinkPartition()
