## Ceiling value
nums = [1, 1, 2, 8, 10, 11, 12, 19]
x = 0

# Edge case: empty array
if not nums:
    print("Array is empty, no ceiling exists")
# Edge case: x is greater than the largest element
elif x > nums[-1]:
    print(f"No ceiling exists for {x} (exceeds max element {nums[-1]})")
# Edge case: x is smaller than or equal to the first element
elif x <= nums[0]:
    print(f"Ceiling of {x} is {nums[0]} (first element itself)")
else:
    # Binary search for the smallest element >= x
    low = 0
    high = len(nums) - 1

    while low < high:
        mid = (low + high) // 2  # Find the middle index

        if nums[mid] >= x:
            high = mid   # Ceiling could be at mid or to the left
        else:
            low = mid + 1  # Ceiling must be to the right

    # At the end, low == high == index of the ceiling element
    print(f"Ceiling of {x} is {nums[high]}")


## First Occurance in a binary 
arr = [0, 2, 1, 0]

# Edge case: array too small to form a mountain
if len(arr) < 3:
    print("Not a valid mountain array (need at least 3 elements)")
else:
    # Peak is guaranteed to not be at the first or last index
    low = 0
    high = len(arr) - 1

    # Template B: converge to the peak index
    # Condition: arr[mid] > arr[mid+1] means we're on the descent or at the peak
    #           arr[mid] < arr[mid+1] means we're still climbing — peak is to the right
    while low < high:
        mid = (low + high) // 2       # left-mid (safe here since we use high = mid)

        if arr[mid] < arr[mid + 1]:   # still ascending — peak must be to the right
            low = mid + 1             # mid is NOT the peak, move past it
        else:                         # descending or at peak — peak could be at mid
            high = mid                # keep mid, search left

    # Post-loop: low == high == peak index
    print(f"Peak is at index {low}, value = {arr[low]}")

# #Peak index in a mountain array
arr = [0, 2, 1, 0]

# Edge case: array too small to form a mountain
if len(arr) < 3:
    print("Not a valid mountain array (need at least 3 elements)")
else:
    # Peak is guaranteed to not be at the first or last index
    low = 0
    high = len(arr) - 1

    # Template B: converge to the peak index
    # Condition: arr[mid] > arr[mid+1] means we're on the descent or at the peak
    #           arr[mid] < arr[mid+1] means we're still climbing — peak is to the right
    while low < high:
        mid = (low + high) // 2       # left-mid (safe here since we use high = mid)

        if arr[mid] < arr[mid + 1]:   # still ascending — peak must be to the right
            low = mid + 1             # mid is NOT the peak, move past it
        else:                         # descending or at peak — peak could be at mid
            high = mid                # keep mid, search left

    # Post-loop: low == high == peak index
    print(f"Peak is at index {low}, value = {arr[low]}")


## Minimum in rotated sorted array
nums = [4, 5, 6, 7, 0, 1, 2]
target = 0

# Edge case: empty array
if not nums:
    print("Array is empty")
else:
    low = 0
    high = len(nums) - 1
    result = -1

    # Template A: low <= high, return mid on exact match
    # Key idea: one half is ALWAYS sorted — figure out which,
    #           then check if target lies in that sorted range
    while low <= high:
        mid = (low + high) // 2

        if nums[mid] == target:                    # found it!
            result = mid
            break

        # Determine which half is sorted
        if nums[low] <= nums[mid]:                 # LEFT half [low..mid] is sorted
            if nums[low] <= target < nums[mid]:    # target is in the sorted left half
                high = mid - 1
            else:                                  # target must be in the right half
                low = mid + 1
        else:                                      # RIGHT half [mid..high] is sorted
            if nums[mid] < target <= nums[high]:   # target is in the sorted right half
                low = mid + 1
            else:                                  # target must be in the left half
                high = mid - 1

    if result != -1:
        print(f"Found {target} at index {result}")
    else:
        print(f"{target} not found in the array")


## SEarch in rotated sorted array

nums = [4, 5, 6, 7, 0, 1, 2]
target = 0

# Edge case: empty array
if not nums:
    print("Array is empty")
else:
    low = 0
    high = len(nums) - 1
    result = -1

    # Template A: low <= high, return mid on exact match
    # Key idea: one half is ALWAYS sorted — figure out which,
    #           then check if target lies in that sorted range
    while low <= high:
        mid = (low + high) // 2

        if nums[mid] == target:                    # found it!
            result = mid
            break

        # Determine which half is sorted
        if nums[low] <= nums[mid]:                 # LEFT half [low..mid] is sorted
            if nums[low] <= target < nums[mid]:    # target is in the sorted left half
                high = mid - 1
            else:                                  # target must be in the right half
                low = mid + 1
        else:                                      # RIGHT half [mid..high] is sorted
            if nums[mid] < target <= nums[high]:   # target is in the sorted right half
                low = mid + 1
            else:                                  # target must be in the left half
                high = mid - 1

    if result != -1:
        print(f"Found {target} at index {result}")
    else:
        print(f"{target} not found in the array")
