# def solve(a):
#     words = a.split()
#     freq = {}
#     for i in words:
#         freq[i] = freq.get(i,0) + 1
#     return freq



# if __name__ == "__main__":
#     a = input()
#     print(solve(a))






# def solve(lst):
#     freq = {}
#     for i in lst:
#         freq[i] = freq.get(i,0) + 1
#     max_freq = max(freq.values())
#     for k in freq:
#         if(freq[k] == max_freq):
#             return k







if __name__ == "__main__":
    n = int(input("How many elements? "))
    tmp = [input("Enter element: ") for _ in range(n)]
    print("Most frequent element:", solve(tmp))
