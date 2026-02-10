import queue

# 创建一个新的队列
q = queue.Queue()

# 将元素添加到队列中
q.put(1)
q.put(2)
q.put(3)

# 清除队列中的所有项
q.queue.clear()

# 检查队列是否为空
if q.empty():
    print("队列已经清空！")
else:
    print("队列不为空！")

