# Databricks notebook source
n=int(input("Enter a number: "))
print("even" if n %2 == 0 else "odd" )

# COMMAND ----------

word = "kkjvhshiiius sfeejvhkj;b"
vowels="aeiou"
count= sum(1 for i in word.lower() if i in vowels)
print(count)

# COMMAND ----------

#rev string
word= "anil kalyan"
rev_word=""
for i in word:
    rev_word=i+rev_word
print(rev_word)

# COMMAND ----------

#rev string
word= "anil kalyan"
rev_word=word[::-1]
print(rev_word)

# COMMAND ----------

#rev string adding spl char
word= "anil kalyan"
rev_word=word[::-1]
rev_word='@'.join(rev_word)
print(rev_word)

# COMMAND ----------

#rev string adding spl char
word= "anil kalyan"
s_word=word.split()
rev_word=s_word[::-1]
print(rev_word)

# COMMAND ----------

#find the largest num
a,b,c=5,7,4
if a>=b and a>=c:
    print(a)
elif b>=a and b>=c:
    print(b)
else:
    print(c)

# COMMAND ----------

#sum of values in str
n=123456
print(sum(int(d) for d in str(n)))
# for i in str(n):
#     print(i)
# print(sum(int(i) for i in str(n)))
#sum of values in str

# COMMAND ----------

# DBTITLE 1,Palindrome or Not
#checking palindrom 
s = "madam"
print("Palindrome" if s == s[::-1] else "Not Palindrome")

# if s==s[::-1]:
#     print("Palindrome")
# else:
#     print("Not Palindrome")

# COMMAND ----------

# DBTITLE 1,Palindrome or Not
def is_palindrome(s):
    return s == s[::-1]

text=input("enter a string:")

if is_palindrome(text):8
    print("Palindrome")
else:
    print("Not Palindrome")

# COMMAND ----------

#finding duplicates in list
arr = [1,2,3,1,2,4,5,5]
out=[]
for i in set(arr):
    #print(i)
    if arr.count(i)>1:
        out.append(i)
print(out)

# COMMAND ----------

arr = [1,2,3,1,2,4,5,5]
out=[]
for i in arr:
    if i not in out:
        out.append(i)
print(out)

# COMMAND ----------

arr = [1,2,3,1,2,4,5,5]
out=[]
out2=[]
for i in arr:
    if i not in out:
        out.append(i)
    else:
        out2.append(i)
print(out)
print(out2)

# COMMAND ----------

d = [i for i in arr if i not in out]
print(d)

# COMMAND ----------

# removing duplicates in list
arr = [1,2,2,3,1]
d=set(arr)
print(list(d))

# COMMAND ----------

# DBTITLE 1,Cell 16
arr = [1,2,2,3,1]
f=sorted(arr)
print(f)

# COMMAND ----------

# removing duplicates in list
arr = [1,2,2,3,1]
out=[]
for i in arr:
    if i not in out:
        out.append(i)
print(out)

# COMMAND ----------

#Find missing number in sequence
arr = [1,2,4,5,9]
missing = set(range(arr[0], arr[-1]+1)) - set(arr)
print(list(missing))  # [3]

# COMMAND ----------

arr = [1,2,4,5,9]
missing = set(range(arr[0], arr[-1]+1))
print(list(missing))

# COMMAND ----------

# removing inner lists
lst = [[1,2],[3,4],[5]]
l=[]
for i in lst:
    for j in i:
        l.append(j)
        #print(j)
print(l)

# COMMAND ----------

# checking list inside the list
lst = [[1, 2], [3, 4], [3, 5], 7, 5]
result=[]
for i in lst:
    if type(i)==list:
        for j in i:
            result.append(j)
    else:
        result.append(i)
print(result)

res1=[]
for i in result:
    if i not in res1:
        res1.append(i)  
print(res1)

# COMMAND ----------

l=[1,2,3,1,1,2,3,4,5]
d={}
for i in l:
    c=l.count(i)
    d[i]=c
print(d)

# COMMAND ----------

# DBTITLE 1,Fibonacci Series Using a Generator
def fibonacci():
    a,b=0,1
    while True:
        yield a
        a,b=b,a+b
gen=fibonacci()
for _ in range(10):
    print(next(gen), end=' ')

#But if you never use i, _ makes code cleaner and more readable.


# COMMAND ----------

# DBTITLE 1,without using yield
def fibonacci(n):
    a,b=0,1
    for _ in range(n):
        print(a, end=" ")
        a,b=b,a+b
fibonacci(5)

# COMMAND ----------

l=range(0,3)
print(l)

# COMMAND ----------

# DBTITLE 1,values stored in list
def fibonacci(n):
    a,b=0,1
    l=[]
    for _ in range(n):
        l.append(a)
        a,b=b,a+b
    print(l)
fibonacci(7)

# COMMAND ----------

n=8
a,b=0,1
count=0
while count<n:
    print(a, end=" ")
    a,b=b,a+b
    count+=1

# COMMAND ----------

l=["anilkaly","anilkalyan","anilkaxc","anilkal"]
min_lenght=min(len(i) for i in l)
s=""
a=l[0]
for i in range(0,min_lenght):
    count=0
    for j in range(0,len(l)):
        if a[i]==l[j][i]:
            count+=1
    if count==len(l):
        s=s+a[i]
    else:
        break
print(s)

# COMMAND ----------

# DBTITLE 1,Sort a List Without Using the sort() Keyword (Bubble Sort)
num=[7,4,7,36,7,6]
# n=num.sort()
# print(num)

s_num=sorted(num)
print(s_num)

# COMMAND ----------

l=["anilkaly","anilkalyan","anilkaxc","anilkal"]
s=sorted(l,key=len)
print(s)

# COMMAND ----------

arr=[11,2,33,54,5,6,7,28,9,10]
n = len(arr)
print(n)
for i in range(n):
    print(i, end=(""))
    for j in range(0, n - i - 1):
        #print(j, end=("")) 
        if arr[j] > arr[j + 1]:
            arr[j], arr[j + 1] = arr[j + 1], arr[j]
print(arr)

# COMMAND ----------

# DBTITLE 1,Factorial of a Number
def factorial(n):
    if n == 0 or n == 1:
        return 1
    else:
        return n * factorial(n - 1)

num = int(input("Enter a number: "))
print("Factorial:", factorial(num))

# COMMAND ----------

num=int(input("Enter a number: "))
fact=1
for i in range(1,num+1):
    fact=fact*i
print(fact)

# COMMAND ----------

# DBTITLE 1,Frequency of Words in a Sentence
from collections import Counter

sentence = input("Enter a sentence: ").lower()
words = sentence.split()

frequency = Counter(words)

print(frequency)


# COMMAND ----------

sentence= input("enter a string:")
words= sentence.split()

frequency={}

for word in words:
    if word in frequency:
        frequency[word]+=1
    else:
        frequency[word]=1
print(frequency)
# print("Word Frequency:")
# for word, count in frequency.items():
#     print(word, ":", count)

# COMMAND ----------

arr = [10, 5, 20, 3, 8]

min_val=min(arr)
max_val=max(arr)
print(min_val,max_val)


# COMMAND ----------

arr = [10, 5, 20, 3, 8]

small_val=arr[0]
large_val=arr[0]

for i in arr:
    if i<small_val:
        small_val=i
    elif i>large_val:
        large_val=i
print(small_val,large_val)

# COMMAND ----------

arr=[11,2,2,33,54,5,6,7,28,9,10]


n=len(arr)
for i in range(n):
    for j in range (0, n-i-1):
        if arr[j] > arr[j+1]:
            arr[j], arr[j+1]=arr[j+1], arr[j]
print(arr)

# COMMAND ----------

n=int(input("enter a number:"))
fact=1
for i in range(1, n+1):
    fact=fact*i
print(fact)

# COMMAND ----------

def fibonacci(n):
    a,b=0,1
    for _ in range(n):
        print(a, end=" ")
        a,b=b,a+b
fibonacci(10)

# COMMAND ----------

a=[2,4,6,8,10,7]
s = list(set(range(a[0], a[-1]+1)) - set(a))
print(s)

# COMMAND ----------

r=[2,4,4,6,7,8,2,10]
d={}
for i in r:
    c=r.count(i)
    d[i]=c
print(d)

# COMMAND ----------

r=[2,4,22,4,6,7,8,2,10]
f=[]
for i in r:
    if i not in f:
        f.append(i)
print(f)

# COMMAND ----------

r=[2,4,4,6,7,8,2,10]
f=[]
for i in set(r):
    if r.count(i)>1:
        f.append(i)
print(f)

# COMMAND ----------

lst = [[1, 2], [3, 4], [3, 5], 7, 5]

r=[]
for i in lst:
    if type(i) == list:
        for j in i:
            r.append(j)
    else:
        r.append(i)
print(r)

r1=[]
for i in r:
    if i not in r1:
        r1.append(i)
print(r1)
