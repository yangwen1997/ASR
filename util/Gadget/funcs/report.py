# -*- coding: utf-8 -*-
from sklearn import metrics
import numpy as np

np.set_printoptions(threshold=10000, linewidth=500)


def pretty_print_np(array):
    shape = array.shape
    max_len = len(str(np.max(array)))
    fmt = '%%%dd' % (max_len + 1)
    for i in range(shape[0]):
        buf = ''
        for j in range(shape[1]):
            buf += fmt % array[i][j]
        print(buf)


def classification_report(y_true, y_pred, labels):
    # classification_report(y_true, y_pred, labels=None, target_names=None, sample_weight=None, digits=2)

    _to_id = dict(zip(labels, range(len(labels))))
    cr = metrics.classification_report([_to_id.get(id_true) for id_true in y_true], 
                                       [_to_id.get(id_pred) for id_pred in y_pred], 
                                       target_names=labels)
    print("# Precision, Recall and F1-Score...")
    print(cr)
    print('\n')

    return cr


def confusion_matrix(y_true, y_pred, labels):
    # confusion_matrix(y_true, y_pred, labels=None, sample_weight=None)

    cm = metrics.confusion_matrix(y_true, y_pred, labels)
    print('# Confusion Matrix')
    # print(cm)
    pretty_print_np(cm)
    print('\n')

    return cm


