# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Array

# COMMAND ----------

# DBTITLE 1,1. largest_element
nums = [2,5,1,3,0]

ans = float('-inf')

for i in nums:
    if i > ans:
        ans = i

print(ans)

# COMMAND ----------

# DBTITLE 1,2. second_largest
nums = [100, 45, 27, 189, -77]

first_max = float('-inf')
second_max = first_max

for i in nums:
    if i> first_max:
        second_max = first_max
        first_max = i
    elif (first_max>i>second_max):
        second_max = i
    else:
        pass

print(second_max)



# COMMAND ----------

# DBTITLE 1,3. Third_largest
nums = [100, 45, 27, 189, 77]
first_max = float('-inf')
second_max = first_max
third_max = second_max

for i in nums:
    if i> first_max:
        third_max = second_max
        second_max = first_max
        first_max = i
    elif (first_max>i>second_max):
        third_max = second_max
        second_max = i
    elif (second_max>i>third_max):
        third_max = i
        # pass

print(first_max)
print(second_max)
print(third_max)

# COMMAND ----------

# DBTITLE 1,4. check _if array is sorted
# nums = [1, 2, 3, 4, 5]
# nums = [4, 3, 2, 1]
# nums = [1, 3, 2, 4]

for i in range(len(nums)-1):
    if nums[i+1]<nums[i]:
        print("Not sorted")
        break



# COMMAND ----------

# DBTITLE 1,5. ** Remove Duplicates from Sorted Array
# Example 1:

# Input: nums = [1,1,2]
# Output: 2, nums = [1,2,_]
# Explanation: Your function should return k = 2, with the first two elements of nums being 1 and 2 respectively.
# It does not matter what you leave beyond the returned k (hence they are underscores).
# Example 2:

# Input: nums = [0,0,1,1,1,2,2,3,3,4]
# Output: 5, nums = [0,1,2,3,4,_,_,_,_,_]
# Explanation: Your function should return k = 5, with the first five elements of nums being 0, 1, 2, 3, and 4 respectively.
# It does not matter what you leave beyond the returned k (hence they are underscores).

i = 0
j = i+1
nums = [0,0,1,1,2]

for j in range(0,len(nums)):
    if nums[i]==nums[j]:
        j+=1

    else:
        nums[i+1] = nums[j]
        j+=1
        i+=1
print(nums)



# COMMAND ----------

# DBTITLE 1,6. Rotate array
nums = [1,2,3,4,5,6,7]
k = 13

k = len(nums)%k

nums = nums[-k:]+nums[:-k]
print(nums)

# COMMAND ----------

# DBTITLE 1,7. move zeros to end
# Input: nums = [0,1,0,3,12]
# Output: [1,3,12,0,0]
nums = [0]
i = 0
j = i
for j in range(len(nums)):
    if nums[j] == 0:
        j+=1
    else:
        nums[i],nums[j] = nums[j],nums[i]
        i+=1
        j+=1
print(nums)


# COMMAND ----------

# DBTITLE 1,8. Union of 2 sorted Arrays
n = 5
m = 5 
arr1 = [1,2,3,4,5]  
arr2 = [2,3,4,4,5]

# Output = [1,2,3,4,5]

print(list(set(arr1[:]+arr2[:])))

# COMMAND ----------

# DBTITLE 1,9. Maximum Consecutive Ones
nums = [1,1,0,1,1,1]

i = 0
j= 0
temp = 0
ans = 0

for j in range(len(nums)):
    if nums[j]==1:
        temp+=1
        # continue
    else:
        temp = 0
    ans = max(ans,temp)

print(ans)




# COMMAND ----------

# DBTITLE 1,10. **Number that appear only once
# Example 2:
# Input: nums = [4,1,2,1,2]
# Output: 4

# Example 3:
# Input: nums = [1]
# Output: 1

ans = 0
for i in nums:
    ans = ans ^ i

return ans

# COMMAND ----------

# DBTITLE 1,11. ** Longest Subarray with given Sum K(Positives) *
# Input: arr[] = [10, 5, 2, 7, 1, -10], k = 15
# Output: 6
# Explanation: Subarrays with sum = 15 are [5, 2, 7, 1], [10, 5] and [10, 5, 2, 7, 1, -10]. The length of the longest subarray with a sum of 15 is 6.


nums = [10, 5, 2, 7, 1, -10]
k = 15

#---Brute Force----
temp = 0
temp_sum = 0
ans = 0

for i in range(len(nums)):
    temp = 1
    temp_sum = nums[i]
    for j in range(i+1,len(nums)):
        temp+=1
        temp_sum += nums[j]
        if temp_sum == k:
            ans = max(ans,temp)

print(ans)

#--------------------

#----optimal----
nums = [10, 5, 2, 7, 1, -10]
k = 15

prefix_sum = 0
mp = {}            # stores: prefix_sum -> index
ans = 0

for i in range(len(nums)):
    prefix_sum += nums[i]

    # Case 1: whole subarray from 0..i equals k
    if prefix_sum == k:
        ans = max(ans, i + 1)

    # Case 2: prefix_sum - k seen before
    if (prefix_sum - k) in mp:
        ans = max(ans, i - mp[prefix_sum - k])

    # store prefix sum index once (we want the earliest index)
    if prefix_sum not in mp:
        mp[prefix_sum] = i

print("Longest Length =", ans)

#----optimal----


mp = {}
prefix_sum = 0
ans = 0

for i in range(len(nums)):
    prefix_sum+=nums[i]
    
    if prefix_sum == k:
        ans = max(ans,i+1)
        print(f"found at 0 to {i+1}")
        
    if (prefix_sum-k) in mp:
        if (i-mp[(prefix_sum-k)]) > ans:
            ans = i-mp[(prefix_sum-k)]
            print(f"found at {i} to {mp[(prefix_sum-k)] + 1}")
    
    if (prefix_sum) not in mp:
        mp[(prefix_sum)] = i

print(ans)
    


# COMMAND ----------

# DBTITLE 1,12. 2 Sum
nums = [3,2,4]
target = 6

d = {}

for i in range(len(nums)):
    if (target - nums[i]) not in d:
        d[nums[i]] = i
    else:
        print(f"The pair is {d[target-nums[i]]} and {i}")
        break

# COMMAND ----------

# DBTITLE 1,13. ** Majority_elements *
nums = [0,1,1]

ans = -1
temp =  ans
temp_cnt = 0
for i in range(len(nums)):
    if nums[i] != ans and temp_cnt == 0:
        ans = nums[i]
        temp_cnt = 1
    elif nums[i]== ans:
        temp_cnt+=1
    else:
        temp_cnt-=1

print(ans)


# COMMAND ----------

# DBTITLE 1,14. ** sort 1,0,2
nums = [2,0,1]
i = 0
j = len(nums)-1
k = 0

while(k<=j):
    if nums[k]==0:
        nums[k], nums[i] = nums[i], nums[k]
        i+=1
        k+=1
    elif nums[k]==2:
        nums[k],nums[j] = nums[j],nums[k]
        j-=1
    else:
        k+=1
print(nums)

# COMMAND ----------

# DBTITLE 1,15. **Kadane's Alog
# Example 1:
# Input:
#  nums = [2, 3, 5, -2, 7, -4]  
# Output:
#  15  
# Explanation:
#  The subarray from index 0 to index 4 has the largest sum = 15, which is the maximum sum of any contiguous subarray.

nums = [2, 3, 5, -2, 7, -4]
#  15 
temp =0
ans =0
start = 0
end = 0

for i in range(len(nums)):
    # temp = max(temp+nums[i],nums[i])
    if temp+nums[i]>nums[i]:
        temp = temp+nums[i]
    else:
        temp = nums[i]
        start = i
        
    if temp> ans:
        end = i
        ans = temp
    # ans = max(ans,temp)
    
print(ans)
print(start)
print(end)


# COMMAND ----------

# DBTITLE 1,16. **Stock Buy And Sell
# prices = [7,1,5,3,6,4]
# # Output: 5
# # Explanation: Buy on day 2 (price = 1) and sell on day 5 (price = 6), profit = 6-1 = 5.
# # Note: That buying on day 2 and selling on day 1 is not allowed because you must buy before you sell.
# mini = float('inf')
# ans = 0

# for i in range(len(prices)):
#     if prices[i]<= mini:
#         mini = prices[i]
#     else:
#         ans = max(ans,(prices[i]-mini))

# print(ans)

# COMMAND ----------

# DBTITLE 1,17. **Longest consecutive numbers
# Input: nums = [100,4,200,1,3,2]
# Output: 4
# Explanation: The longest consecutive elements sequence is [1, 2, 3, 4]. Therefore its length is 4.
# Example 2:

# Input: nums = [0,3,7,2,5,8,4,6,0,1]
# Output: 9

def longestConsecutive(self, nums: List[int]) -> int:
    if len(nums)==0:
        return 0
    st  = set(nums)
    ans = 1
    temp_ans = 0

    for i in st:
        if i-1 not in st:
            start  = i
            temp_ans = 1
            
            while(start+1 in st):
                temp_ans+=1
                start = start+1
        ans = max(temp_ans,ans)

    return(ans)

# COMMAND ----------

# DBTITLE 1,18. *Give all the subarrays with given sum
nums = [3, 1, 2, 4]
ans = []
k = 6
mp = {}

prefix_sum = 0
for i in range(len(nums)):
    prefix_sum+=nums[i]
    
    if prefix_sum== k:
        ans.append(nums[0:i+1])
        
    if prefix_sum-k in mp:
        start = mp[prefix_sum - k] + 1
        ans.append(nums[start:i+1])

        
    if prefix_sum not in mp:
        mp[prefix_sum] = i

print(ans)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Binary_search

# COMMAND ----------

# DBTITLE 1,Bineary_search
a = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91]
target = 382


def recursive_binary(nums, target,low,high):
    if low>high:
        return -1
    
    mid = (high+low)//2

    if nums[mid]== target:
        print(f"found at {mid}")
        return mid
    elif target>nums[mid]:
        recursive_binary(nums, target, mid+1,high)
    else:
        recursive_binary(nums, target, low,mid-1)

def iterative_binary(nums, target):
    low = 0
    high = len(nums)

    while(low<high):
        mid = (low + high)//2
        if nums[mid]== target:
            print(f"found at {mid}")
            break
        elif target>nums[mid]:
            low = mid +1
        else:
            high = mid-1
    else:
        print("Not found")


recursive_binary(a, target,0,len(a)-1)
iterative_binary(a,target)


    

# COMMAND ----------

# DBTITLE 1,Lower_bound
nums= [2, 3, 7, 10, 11, 11, 25]
target = 9

low = 0
high = len(nums)-1
ans = len(nums)

while(low<=high):
    mid = (low+high)//2

    if x <= nums[mid]:
        ans =  mid
        high = mid-1

    else:
        low = mid+1




print(mid) 
    


# COMMAND ----------

# DBTITLE 1,Serach insert pos
nums = [1,2,4,7]
x = 6

low = 0
high = len(nums)-1
ans = len(nums)

while(low<=high):
    mid = (low+high)//2
    if nums[mid]>=x:
        high = mid-1
        ans = mid
    else:
        low = mid+1

print(ans)



# COMMAND ----------

# DBTITLE 1,Search in sorted rotated array**
nums = [7, 8, 1, 2, 3, 3, 3, 4, 5, 6]
target = 3
# arr = [7, 8, 1, 2, 3, 3, 3, 4, 5, 6], k = 10
low = 0
high = len(nums)-1



# Run loop while valid search space exists
while low <= high:
    # Find the middle index
    mid = (low + high) // 2

    # If target found at mid, return index
    if nums[mid] == target:
        print(mid)

    # Check if left half is sorted
    if nums[low] <= nums[mid]:
        # If target lies in left half
        if nums[low] <= target < nums[mid]:
            high = mid - 1
        else:
            low = mid + 1
    else:
        # Right half is sorted
        if nums[mid] < target <= nums[high]:
            low = mid + 1
        else:
            high = mid - 1

# COMMAND ----------

prices = [1,2]     
temp = prices[0]
ans = 0


for i in range(1,len(prices)):
    if prices[i]<temp:
        temp = prices[i]
    else:
        ans = max(ans, prices[i]-temp)

print(ans)

# COMMAND ----------

print(set([100,4,200,1,3,2]))
nums_set = set([100,4,200,1,3,2])
temp_cnt = 1
ans = temp_cnt

for i in nums_set:
    if (i-1) not in nums_set:
        temp_cnt=1
    else:
        temp_cnt +=1
    ans = max(temp_cnt,ans)

print(ans)


# COMMAND ----------

# DBTITLE 1,find the min_array
nums = [1,3,4,5,6]
# nums = [1]

if len(nums)==0 or len(nums)==1:
    print(*nums)

low = 0
high = len(nums)-1
while(low<high):
    mid = (low+high)//2

    if nums[mid+1]<nums[mid]:
        print(nums[mid+1])
        break

    if nums[mid-1]>mid:
        print(nums[mid])
        break

    if nums[mid]>nums[low]:
        low = mid+1
    else:
        high = mid-1





# COMMAND ----------

# DBTITLE 1,*IMPLEMENT LOWER BOUND
#Implementing lower bound, when was the first occurenece of the number
nums = [3, 4, 13, 13, 13, 20, 40] 
x = 13

low = 0
high = len(nums)-1
ans = len(nums)

while(low<high):
    mid = (low+high)//2

    if x<=nums[mid]:
        ans = mid
        high = mid-1
    else:
        low = mid+1

print(f"first_occurence at {ans}")

low = 0
high = len(nums)-1
ans = len(nums)
while(low<high):
    mid = (low+high)//2

    if x>=nums[mid]:
        
        low = mid+1
        ans = low
        
    else:
        high = mid-1

print(ans)

# COMMAND ----------

S = "abcddabac"
window = 0
ans = window
temp_s=''

for i in S:
    if temp_s == '' and window == 0:
        temp_s = i
        window +=1
    
    if i == temp_s:
        window+=1
    else:
        temp_s = ''
        window = 1
    


# COMMAND ----------

s = "abac"
liat = [0]*26

ans = 1
i  = 0
j = i+1
liat[ord(s[i])-97] = 1

while(j<len(s)):
    if liat[ord(s[j])-97]==1:
        i = i+1
        # liat[ord(s[i])-97] = 1
    else:
        liat[ord(s[j])-97] = 1
        ans = max(ans,j-i+1)
        j+=1

print(ans)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sliding windnow

# COMMAND ----------

# DBTITLE 1,1. Length of longest substring without any repeating char
# Example 1:
# Input:
#  S = "abcddabac"  
# Output:
#  4  
# Explanation:
#  The longest substring with distinct characters is "abcd", which has a length of 4.

# Example 2:
# Input:
#  S = "aaabbbccc"  
# Output:
#  2  
# Explanation:
#  The longest substrings with distinct characters are "ab" and "bc", both having a length of 2.

def lengthOfLongestSubstring(self, s: str) -> int:
    
    if len(s)==0:
        return 0
    if len(s)==1:
        return 1
    s_list = [0]*256

    ans = 0
    l_ptr=0
    s_list[ord(s[l_ptr])-97] = 1

    for r_ptr in range(1,len(s)):
        s_list[ord(s[r_ptr])-97]+=1
        while(s_list[ord(s[r_ptr])-97]>1):
            s_list[ord(s[l_ptr])-97]-=1
            l_ptr +=1
            
        ans = max(ans,(r_ptr-l_ptr+1))

    return ans

# COMMAND ----------

# DBTITLE 1,**2. longest-substring-with-at-most-k-distinct-characters
# Example 1:

# Input: s = "eceba", k = 2
# Output: 3
# Explanation: The substring is "ece" with length 3.
# Example 2:

# Input: s = "aa", k = 1
# Output: 2
# Explanation: The substring is "aa" with length 2.

def lengthOfLongestSubstringKDistinct(self, s: str, k: int) -> int:

    if len(s)==0:
        return 0
    if len(s)==1:
        if k>0:
            return 1
        else:
            return 0

    mp = {}

    ans = 0
    l_ptr = 0

    for r_ptr in range(len(s)):
        if(s[r_ptr] not in mp):
            mp[s[r_ptr]] = 1
        else:
            mp[s[r_ptr]] += 1
        while(len(mp.keys())>k):
            # Correct logic (decrement first, then delete)
            mp[s[l_ptr]] -= 1          
            if mp[s[l_ptr]] == 0:      
                del mp[s[l_ptr]]
            l_ptr += 1

        ans = max(ans,(r_ptr-l_ptr+1))
        
    return(ans)