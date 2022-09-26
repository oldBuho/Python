# rotate a list a #k amount of positions
# eg: [1,2,3,4,5] and k=3 -> result: [3,4,5,1,2]

def rotate(nums, k:int):
    i = 1
    while i <= k:
        i += 1
        nums.insert(0, nums[-1])
        nums.pop()
    return nums

print(rotate([1,2,3,4,5], 3))