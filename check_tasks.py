import asyncio

async def main():
    # Check for pending tasks
    tasks = asyncio.all_tasks()
    print(f'Pending tasks: {len(tasks)}')
    for task in tasks:
        print(f'  {task.get_name()}: {task.get_coro()}')

asyncio.run(main())
