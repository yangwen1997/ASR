#coding=utf-8

#剔除有包含关系的子项
def del_sub_str(srcli):

    substrs = []
    size = len(srcli)
    for i in range(0,size):
        for j in range(0,size):
            if i == j:
                continue
            else:
                if len(srcli[i])>len(srcli[j]):
                    if srcli[j] in srcli[i]:
                        substrs.append(srcli[j])
                    elif srcli[i] in srcli[j]:
                         .append(srcli[i])
    srclist = list(set(srcli).difference(set(substrs)))
    if "现金" in  srcli and not "现金" in srclist:
        srclist.append("现金")
    if "人民币" in srclist  and "现金" in srclist:
        srclist.remove("人民币")
    if "人民币现金" in srclist  and "现金" in srclist:
        srclist.remove("人民币现金")
    if "现金人民币" in srclist  and "现金" in srclist:
        srclist.remove("现金人民币")
    return srclist


# srcli = ["aaaaa",'aa',"bb",'aaaa','aabb']
# srcli = ['三中路恒达巴士门口', '解放北路中医院','解放北路中医院20号']
# print(del_sub_str(srcli))