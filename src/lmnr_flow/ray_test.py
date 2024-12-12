import ray
from ray.util.queue import Queue
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# 初始化 Ray
ray.init()

# 定义生产者任务
@ray.remote
def producer(queue, n):
    for i in range(n):
        queue.put(f"Data-{i}")
    queue.put(None)  # 使用 None 标记结束

# 定义消费者任务
@ray.remote
def consumer(queue):
    while True:
        item = queue.get()
        if item is None:  # 检测结束标志
            break
        logging.info(f"Consumed: {item}")

if __name__ == "__main__":
    # 创建一个共享队列
    queue = Queue(maxsize=10)

    # 启动生产者和消费者
    producer_task = producer.remote(queue, 10)
    consumer_task = consumer.remote(queue)

    # 等待任务完成
    ray.get([producer_task, consumer_task])

    # 关闭 Ray
    ray.shutdown()