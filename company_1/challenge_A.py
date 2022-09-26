'''
Given a S string, return the "reversed" string where all characters
that are not a letter stay in the same place, and all letters reverse
their positions.
eg:
input "a-bC-dEf=ghlj!!" 
output "j-lh-gfE=dCba!!" 
'''

def reverseOnlyLetters(S:str):
    
    alNum =  [char for char in S if char.isalnum() == True]
    #another option -> alNum = list(''.join(char for char in S if char.isalnum() == True))
    
    symbols =  [char for char in S if char.isalnum() == False]
    #another option -> symbols = list(''.join(char for char in S if char.isalnum() == False))

    alNum_reversed = list(reversed(alNum))
    # if not redefined as list(), after reverse(), error pops up like this: 
    #   "object type "list_reverseiterator" has no len()
    #       note: see line 27
    
    result = []
    for char in S:
        if char.isalnum() == True:
            result.append(alNum_reversed[-len(alNum_reversed)]) #first position
            alNum_reversed.pop(-len(alNum_reversed))
        else:
            result.append(symbols[-len(symbols)]) #first position
            symbols.pop(-len(symbols))        
    new_S = ''.join(result)
    return new_S

print(reverseOnlyLetters("a-bC-dEf=ghlj!!"))


'''
notes:
remove all non-alphanum: 
    https://bobbyhadz.com/blog/python-remove-non-alphanumeric-characters-from-string
    new_str = re.sub(r'[\W_]', '', my_str)
'''
