#coding=utf-8
import xlwt

def job(infile, outfile):
    xls_out = xlwt.Workbook(encoding='utf-8')
    sheet_out = xls_out.add_sheet('Sheet1')

    with open(infile) as f:
        row = 0
        for line in f.readlines():
            try:
                label, content = line.strip().split('\t')
                sheet_out.write(row, 0, label)
                sheet_out.write(row, 1, content)
                row += 1
            except Exception as e:
                print('err', e)
                pass

    xls_out.save(outfile)

def datasetwrite(dataset,outpufile):
    xls_out = xlwt.Workbook(encoding='utf-8')
    sheet_out = xls_out.add_sheet('Sheet1')
    row = 0
    for line in dataset:
        try:
            src_label,pre_label,content = line.strip().split('\t')
            sheet_out.write(row,0,src_label)
            sheet_out.write(row,1,pre_label)
            sheet_out.write(row,2,content)
            row += 1
        except Exception as e:
            print('err', e)
            pass
    xls_out.save(outpufile)

def ner_datasetwrite(dataset, outpufile):
    xls_out = xlwt.Workbook(encoding='utf-8')
    sheet_out = xls_out.add_sheet('Sheet1')
    row = 0
    for line in dataset:
        try:
            ner_ent, content = line.strip().split('\t')
            sheet_out.write(row, 0, ner_ent)
            sheet_out.write(row, 1, content)
            row += 1
        except Exception as e:
            print('err', e)
            pass
    xls_out.save(outpufile)


    #job('/home/joe/DL/iba/dmp/gongan/shandong_crim_classify/10w_result.txt', '/home/joe/sd_10w.xls')