# -*- coding: utf-8 -*-

_FH_SPACE = ((u'　', u' '),)
_HF_SPACE = ((u" ", u"　"),)

_FH_NUM = (
    (u'０', u'0'), (u'１', u'1'), (u'２', u'2'), (u'３', u'3'), (u'４', u'4'),
    (u'５', u'5'), (u'６', u'6'), (u'７', u'7'), (u'８', u'8'), (u'９', u'9'),
)
_HF_NUM = lambda: ((h, z) for z, h in _FH_NUM)

_FH_ALPHA = (
    (u'ａ', u'a'), (u'ｂ', u'b'), (u'ｃ', u'c'), (u'ｄ', u'd'), (u'ｅ', u'e'),
    (u'ｆ', u'f'), (u'ｇ', u'g'), (u'ｈ', u'h'), (u'ｉ', u'i'), (u'ｊ', u'j'),
    (u'ｋ', u'k'), (u'ｌ', u'l'), (u'ｍ', u'm'), (u'ｎ', u'n'), (u'ｏ', u'o'),
    (u'ｐ', u'p'), (u'ｑ', u'q'), (u'ｒ', u'r'), (u'ｓ', u's'), (u'ｔ', u't'),
    (u'ｕ', u'u'), (u'ｖ', u'v'), (u'ｗ', u'w'), (u'ｘ', u'x'), (u'ｙ', u'y'), (u'ｚ', u'z'),
    (u'Ａ', u'A'), (u'Ｂ', u'B'), (u'Ｃ', u'C'), (u'Ｄ', u'D'), (u'Ｅ', u'E'),
    (u'Ｆ', u'F'), (u'Ｇ', u'G'), (u'Ｈ', u'H'), (u'Ｉ', u'I'), (u'Ｊ', u'J'),
    (u'Ｋ', u'K'), (u'Ｌ', u'L'), (u'Ｍ', u'M'), (u'Ｎ', u'N'), (u'Ｏ', u'O'),
    (u'Ｐ', u'P'), (u'Ｑ', u'Q'), (u'Ｒ', u'R'), (u'Ｓ', u'S'), (u'Ｔ', u'T'),
    (u'Ｕ', u'U'), (u'Ｖ', u'V'), (u'Ｗ', u'W'), (u'Ｘ', u'X'), (u'Ｙ', u'Y'), (u'Ｚ', u'Z'),
)
_HF_ALPHA = lambda: ((h, z) for z, h in _FH_ALPHA)

_FH_PUNCTUATION = (
    (u'．', u'.'), (u'，', u','), (u'！', u'!'), (u'？', u'?'), (u'”', u'"'),
    (u'’', u'\''), (u'‘', u'`'), (u'＠', u'@'), (u'＿', u'_'), (u'：', u':'),
    (u'；', u';'), (u'＃', u'#'), (u'＄', u'$'), (u'％', u'%'), (u'＆', u'&'),
    (u'（', u'('), (u'）', u')'), (u'‐', u'-'), (u'＝', u'='), (u'＊', u'*'),
    (u'＋', u'+'), (u'－', u'-'), (u'／', u'/'), (u'＜', u'<'), (u'＞', u'>'),
    (u'［', u'['), (u'￥', u'\\'), (u'］', u']'), (u'＾', u'^'), (u'｛', u'{'),
    (u'｜', u'|'), (u'｝', u'}'), (u'～', u'~'), (u'。', u'.'), # (u'・', u'・'), 
    (u'「', u']'), (u'」', u']'), (u'【', u'['), (u'】', u']'), (u'、', u','),
    (u'—', u'-'),
)
_HF_PUNCTUATION = lambda: ((h, z) for z, h in _FH_PUNCTUATION)

_FH_ASCII = lambda: ((fr, to) for m in (_FH_ALPHA, _FH_NUM, _FH_PUNCTUATION, _FH_SPACE) for fr, to in m)
_HF_ASCII = lambda: ((h, z) for z, h in _FH_ASCII())


def _f2h_char(char):
    for x in _FH_ASCII():
        if char == x[0]:
            return x[1]
    return char

def _h2f_char(char):
    for x in _HF_ASCII():
        if char == x[0]:
            return x[1]
    return char

def hf_convert(content, direct='h2f'):
    out_str = ''
    for c in content:
        if direct == 'h2f':
            out_str += _h2f_char(c)
        elif direct == 'f2h':
            out_str += _f2h_char(c)
        else:
            raise Exception

    return out_str


if __name__ == '__main__':
    text = [
            u"成田空港—【ＪＲ特急成田エクスプレス号・横浜行，2站】—東京—【ＪＲ新幹線はやぶさ号・新青森行,6站 】—新青森—【ＪＲ特急スーパー白鳥号・函館行，4站 】—函館",
            '汉字字符和规定了全角的英文字符及国标ＧＢ２３１２－８０中的图形符号和特殊字符都是全角字符。一般的系统命令是不用全角字符的，只是在作文字处理时才会使用全角字符。　半角－－－指一字符占用一个标准的字符位置。　通常的英文字母、数字键、符号键都是半角的，半角的显示内码都是一个字节。在系统内部，以上三种字符是作为基本代码处理的，所以用户输入命令和参数时一般都使用半角．全角占两个字节，半角占一个字节。半角全角主要是针对标点符号来说的，全角标点占两个字节，半角占一个字节，而不管是半角还是全角，汉字都还是要占两个字节　在编程序的源代码中只能使用半角标点（不包括字符串内部的数据）　在不支持汉字等语言的计算机上只能使用半角标点．　例如：在半角状态下打的句号只是一个圆点，而在全角下就是标准的句号。', 
    ]
    for s in text:
        str_h = hf_convert(s, 'f2h')
        str_f = hf_convert(str_h, 'h2f')
        print('Half :', str_h)
        print('Full:', str_f)
        print('\n'*2)

