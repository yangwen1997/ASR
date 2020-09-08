from .funcs.brackets import filter_brackets, pop_brackets
from .funcs.full_half_width import hf_convert 
from .funcs.loadfile import PathSentences, FileSentences
from .funcs.report import classification_report, confusion_matrix
from .funcs.label_map import load_json


class GadgetBox(object):
    def __init__(self):
        pass

    @staticmethod
    def hf_convert(content, direct='h2f'):
        return hf_convert(content, direct)

    @staticmethod
    def brackets_filter(content):
        return filter_brackets(content)

    @staticmethod
    def brackets_pop(content):
        return pop_brackets(content)

    @staticmethod
    def report_classification(y_true, y_pred, labels):
        classification_report(y_true, y_pred, labels)

    @staticmethod
    def report_confusion_matrix(y_true, y_pred, labels):
        confusion_matrix(y_true, y_pred, labels)

    @staticmethod
    def label_map(filename):
        return load_json(filename)



