#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/12/7 上午10:40

'''
给定一个整数数组 nums 和一个目标值 target，请你在该数组中找出和为目标值的那两个整数，并返回他们的数组下标。
你可以假设每种输入只会对应一个答案。但是，数组中同一个元素不能使用两遍。
[示例]
给定 nums = [2, 7, 11, 15], target = 9
因为 nums[0] + nums[1] = 2 + 7 = 9
所以返回 [0, 1]
'''


class Solution:
    # 强行遍历，效率低
    @classmethod
    def twoSum_2for(cls, nums, target):
        for x in range(len(nums)):
            for y in range(x, len(nums)):
                if nums[x] + nums[y] == target:
                    return [x, y]

    # 利用字典
    @classmethod
    def twoSum_dict(cls, nums, target):
        kvs = {}
        for index, value in enumerate(nums):
            another_value = target - value
            if another_value in kvs.values():
                return [list(kvs.values()).index(another_value), index]
            kvs[index] = value


if __name__ == '__main__':
    # l = Solution.twoSum_2for([2, 7, 11, 15], 18)
    l = Solution.twoSum_dict([2, 7, 11, 15], 9)
    print(l)
