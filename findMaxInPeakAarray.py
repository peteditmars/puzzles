'''
Given an array of integers which is initially increasing and then decreasing, find the maximum value in the array.
Examples :
Input: arr[] = {8, 10, 20, 80, 100, 200, 400, 500, 3000, 2, 1}
Output: 3000
Input: arr[] = {1, 3, 50, 10, 9, 7, 6}
Output: 50

Write this to be able to scale to arrays with many thousands of elements, so we'll use actual "array" type not list

Investigate the following:
Does breaking very large arrays into subproblems allow us to process large arrays faster in parallel
     a. with regular python threads
     b. with subprocesses

Expectations :
a) Working , efficient code
b) cannot use any inbuilt programming language packages
c) Lets cap max time to solve - 35 minutes

'''

import array
import concurrent.futures
import timeit

def generate_array(size, peak):
    '''
    Generate a test array (true array) of specified size, increasing up to peak, then decreasing, and return it
    :param size: size of array
    :param peak: index of where peak/max element resides
    :return: generated array
    '''
    arr = array.array('i', [0] * size)
    for i in range(peak):
        arr[i] = i
    arr[peak] = peak
    val = peak -1
    for i in range(peak + 1, size):
        arr[i] = val
        val = val - 1
    return arr

ga1 = generate_array(10000, 5023)

ga2 = generate_array(100000, 50230)

ga3 = generate_array(1000000, 502300)

ga4 = generate_array(10000000, 5023000)

tests = [ga1, ga2, ga3, ga4]

def findMaxValue(arr, leftmost, rightmost):
    '''
    Find the max value within the designated portion of array
    :param arr: the array to search
    :param leftmost: leftmost index
    :param rightmost: rightmost index
    :return: max value for this portion of array
    '''
    # try using binary search to find the max in the list
    max_index = leftmost
    max_value = None

    # start with left and right at extremes of array and do a binary search looking for a potential max
    left = leftmost
    right = rightmost - 1
    while right > left:
        length = right - left + 1
        possible_max_index = left + (length // 2)
        possible_max_value = arr[possible_max_index]
        if max_value is None or possible_max_value > max_value:
            max_index = possible_max_index
            max_value = possible_max_value
        # handle edge condition of right = left + 1
        # we just tested right... so just test left and be done
        if right == left + 1:
            if arr[left] > max_value:
                max_index = left
                max_value = arr[left]
            break
        # this is an array with a "peak" (sorted ascending then descending)
        # test to left of max_index to see if array is ascending
        if possible_max_index != 0 and arr[possible_max_index - 1] > arr[possible_max_index]:
            # we are in the descending side of the array... so we actually want to move
            # right to possible_max_index and continue the search to the left
            right = possible_max_index
        else:
            # we are on the ascending side of the array... so go ahead and move
            # left to possible_max_index and continue the search to the right
            left = possible_max_index

    return max_value

def findMaxValueThreaded(arr, num_threads, subprocesses=False):
    '''
    Find max value in array that has ascending and descending portions by breaking work into chunks
    and using threading or subprocess/multiprocessing to execute in parallel

    :param arr: array to search
    :param num_threads: number of threads to use
    :return: max value for entire array
    '''

    # handle bad input gracefully
    if num_threads == 0:
        num_threads = 1
    if num_threads > len(arr):
        num_threads = 1

    # we don't know overall max value yet
    overall_max_value = None

    # Start threads for each link giving each thread a chunk of the array to deal with
    threads = []
    leftmost = 0
    chunk_size = len(arr) // num_threads
    rightmost = chunk_size - 1
    if subprocesses:
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_threads)
    else:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
    futures = []
    for i in range(num_threads):
        if i == num_threads - 1:
            # handle last chunk case
            rightmost = len(arr) - 1
        future = executor.submit(findMaxValue, arr, leftmost, rightmost)
        futures.append(future)
        leftmost += chunk_size
        rightmost += chunk_size

    # get the result from each thread
    # the max of the results is the overall max
    for future in futures:
        max_value = future.result()
        if overall_max_value is None or max_value > overall_max_value:
            overall_max_value = max_value

    executor.shutdown(wait=True)

    return overall_max_value


class CaptureReturnValue:
    def __init__(self, func):
        self.func = func
        self.return_value = None

    def __call__(self, *args, **kwargs):
        self.return_value = self.func(*args, **kwargs)
        return self.return_value

crv = CaptureReturnValue(findMaxValue)

for test in tests:
    elapsed = timeit.timeit(lambda: crv(test, 0, len(test) - 1), globals=globals(), number=10)
    print(f"elapsed time = {elapsed:.6f}  not threaded array size={len(test)} ans={crv.return_value}")

crv = CaptureReturnValue(findMaxValueThreaded)

num_threads_list = [1]
for num_threads in num_threads_list:
    for subprocesses in [False, True]:
        for test in tests:
            elapsed = timeit.timeit(lambda: crv(test, num_threads, subprocesses), globals=globals(), number=10)
            pool_type = "subprocesses" if subprocesses else "threads"
            print(f"elapsed time = {elapsed:.6f}  {num_threads} {pool_type} array size={len(test)} ans={crv.return_value}")
