# -*- coding: utf-8 -*-

SYMBOLS = {'}': '{', ']': '[', ')': '(', '>': '<', '）': '（', '】': '【'}
SYMBOLS_L, SYMBOLS_R = SYMBOLS.values(), SYMBOLS.keys()


def filter_brackets(content):
    """
    删除括号及括号内的内容
    :param content:
    :return: True/False, NewContent
    """
    out = []
    _arr_symbols = []
    for c in content:
        if c in SYMBOLS_L:
            # 左符号入栈
            _arr_symbols.append(c)
        elif c in SYMBOLS_R:
            # 右符号要么出栈，要么匹配失败
            if _arr_symbols and _arr_symbols[-1] == SYMBOLS[c]:
                _arr_symbols.pop()
            else:
                return False, content

        if not _arr_symbols and c not in SYMBOLS_R:
            out.append(c)

    return True, ''.join(out)


def pop_brackets(content):
    out = []
    _arr_symbols = []
    arr_bracket = []

    for i in range(len(content)):
        c = content[i]
        if c in SYMBOLS_L:
            # 左符号入栈
            _arr_symbols.append(c)
            _pos_l = i + 1
        elif c in SYMBOLS_R:
            # 右符号要么出栈，要么匹配失败
            if _arr_symbols and _arr_symbols[-1] == SYMBOLS[c]:
                _arr_symbols.pop()
                if arr_bracket:
                    out.append((_pos_l, ''.join(arr_bracket)))
                    arr_bracket.clear()
            else:
                # FIXME
                pass
        elif _arr_symbols:
            arr_bracket.append(c)

    return out


if __name__ == '__main__':
    s = '一女子称家中被盗。2017年12月14日6时51分，一女子（联系电话：18615947037）报称： 家中被盗。民警刘铭顺、袁凤鸣到达现场后反馈：李林玉（女 ，年龄：28，身份证号：370683198902142627，户籍地：莱州市朱桥镇，现住地：幸国里21-5号，职业：无，无特殊身份，联系电话：18615947037），报称早6时许发现家中被盗，被盗钻戒一枚，价值1000元，黄金戒指一枚，价值1000元，阿玛尼手表一块，价值3000元，杂牌手表一块，价值300元。作案手段：爬阳台入室盗窃。2017年12月14日7时许，李林玉报案称其在山东省烟台市芝罘区幸国里21-5家中的，钻戒一枚，价值1000元，八成新，黄金戒指一枚，价值1000元，阿玛尼手表一块，价值3000元，9成新，杂牌手表一块，价值300元，九成新，被盗物品总价值约4800元人民币。'
    print(s)
    print('\n')
    print(filter_brackets(s))
    print('\n')
    print(len(pop_brackets(s)), pop_brackets(s))
