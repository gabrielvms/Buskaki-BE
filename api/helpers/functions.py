from multiset import Multiset

def jaccard_similarity(string1, string2):
    set1 = Multiset(string1.lower())
    set2 = Multiset(string2.lower())
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union)
    
def dice_coefficient(string1, string2):
    set1 = Multiset(string1.lower())
    set2 = Multiset(string2.lower())
    intersection = set1.intersection(set2)
    coefficient = (2 * len(intersection)) / (len(set1) + len(set2))
    return coefficient

