import sys
import pandas as pd
import os
import os.path
wd = os.getcwd()
sys.path.append(wd)
from collections import namedtuple
from . import taggedString as ts
from . import handleQuery as hq
from . import cqlutil as cu
import sys
from PyQt5.QtWidgets import (QWidget, QPushButton, QLabel, QLineEdit, QTextBrowser, QAbstractScrollArea,
    QHBoxLayout, QVBoxLayout, QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QAbstractItemView,
    QShortcut,QInputDialog,QTextEdit,QProgressBar,QComboBox,QFileDialog)
from PyQt5.QtGui import QKeySequence
import re
#from PyQt5.QtCore import Qt, pyqtSignal
#import shelve


class cqlDialog(QWidget):
    """ Displays a dialog where the user can enter a CQL query, which is executed against a 
        taggedStringStore. The results are displayed in a KWIC-like layout"""
    
    def __init__(self,tssstore, elindex):
        super().__init__()
        self.tsqh = hq.tsQueryHelper('word')
        tss = ts.taggedStringStore(tssstore)
        self.tssShelve = tss.tssOpenRead()
        self.elindex = elindex
        self.mrs = {}
#        for k in self.tssShelve.keys():
#            kp = k.partition('.')
#            if kp[0] in self.mrs:
#                self.mrs[kp[0]].append(k)
#            else:
#                self.mrs[kp[0]] = [k]
        self.expdir = None
        self.initUI()
        
    def initUI(self):

        cqlLbl = QLabel('CQL', self)
        self.cqlEdit = QLineEdit("")
        self.cqlHistCombo = QComboBox()
        self.cqlHistCombo.setMinimumContentsLength(40)
        self.cqlHistCombo.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLength)
        selButton = QPushButton("Se&lect")
        selButton.clicked.connect(self.selhist) 

#        self.cqlEdit.textChanged.connect(self.cqlCheck)
        
        self.cqlhBox = QHBoxLayout()
#        self.cqlvBox.addWidget(cqlLbl)
        self.cqlhBox.addWidget(self.cqlEdit)
        self.cqlhBox.addWidget(self.cqlHistCombo)
        self.cqlhBox.addWidget(selButton)

        clearButton = QPushButton("&Clear")
        clearButton.clicked.connect(self.clearcqlvBox)            

        checkButton = QPushButton("C&heck")
        checkButton.clicked.connect(self.cqlCheck)            

        newButton = QPushButton("New &group")
        newButton.clicked.connect(self.newCQLGroup)

        newempButton = QPushButton("New empt&y group")
        newempButton.clicked.connect(self.newempCQLGroup)

        execButton = QPushButton("&Exec")
        execButton.clicked.connect(self.cqlExec)            
        
        buttonhBox = QHBoxLayout()
        buttonhBox.addStretch(1)
        buttonhBox.addWidget(newButton)
        buttonhBox.addWidget(newempButton)
        buttonhBox.addWidget(checkButton)
        buttonhBox.addWidget(clearButton)
        buttonhBox.addWidget(execButton)

        browservBox = QVBoxLayout()
        
        self.resTable = QTableWidget(0, 4, self)
        self.resTable.setHorizontalHeaderLabels(['meta','pre','hit','post'])
        self.resTable.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
#        self.resTable.setGeometry(0, 0, 800, 200)
        self.resTable.setColumnWidth(0,200)
        self.resTable.setColumnWidth(1,300)
        self.resTable.setColumnWidth(2,200)
        self.resTable.setColumnWidth(3,300)
        self.resTable.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.resTable.itemDoubleClicked.connect(self.moreInfo)
        browservBox.addWidget(self.resTable)

#        saveButton = QPushButton("&Save CQL command and code")
#        saveButton.clicked.connect(self.saveExec)            
        
        expButton = QPushButton("E&xport")
        expButton.clicked.connect(self.expExec)            
        
        sbuttonhBox = QHBoxLayout()
        sbuttonhBox.addStretch(1)
        sbuttonhBox.addWidget(expButton)
#        sbuttonhBox.addWidget(saveButton)

        vbox = QVBoxLayout()
#        vbox.addStretch(1)
        vbox.addWidget(cqlLbl)
        vbox.addLayout(self.cqlhBox)
        vbox.addLayout(buttonhBox)
        vbox.addLayout(browservBox)
        vbox.addLayout(sbuttonhBox)
        
        self.setLayout(vbox)

    def moreInfo(self,item):
        ident = self.resTable.verticalHeaderItem(item.row()).text()
        key = ident.partition('.')[0]
        self.ex.responseLbl.setText('Extra info about responseid ' + key)
        self.ex.taggedTE.setPlainText(self.tssShelve[ident].flatstring())
        i = 1
        string = ''
        while key + '.' + str(i) in self.tssShelve:
            string += ' '.join(self.tssShelve[key + '.' + str(i)].getwords()) + '\n' 
            i += 1
#        string = '\n'.join(' '.join(self.tssShelve[k].getwords()) for k in self.mrs[key])
        self.ex.reviewTE.setPlainText(string)
        self.ex.show()
        self.ex.activateWindow()

    def selhist(self):
        if self.cqlHistCombo.count() > 0:
            if self.cqlHistCombo.currentIndex() < 0:
                self.cqlHistCombo.setCurrentIndex = 0
            self.cqlEdit.setText(self.cqlHistCombo.itemText(self.cqlHistCombo.currentIndex()))
        else:
            self.parent().statusBar().showMessage('No items in cql history')
            return True
        self.cqlExec()
        return True

    def clearcqlvBox(self):
        self.cqlEdit.setText('')

    def cqlCheck(self):
        self.cqlEdit.setStyleSheet("color: rgb(0, 0, 0);")
        self.parent().statusBar().showMessage('')
        try:
            self.tsqh.translate(self.cqlEdit.text())
        except cu.InputError as error:
            self.parent().statusBar().showMessage(str(error))
            self.cqlEdit.setStyleSheet("color: rgb(255, 0, 0);")
            return False
        return True

    def codeCheck(self):
        self.codeEdit.setStyleSheet("color: rgb(0, 0, 0);")
        self.parent().statusBar().showMessage('')
        patt = '[A-L](-[A-L])?\s([1-9]|1[0-9]|20|21)(-([1-9]|(1[0-9]|20|21)))?(\s(1!|1|0|-1|-1!))?(\s(p|u|n))?(\s(r|a|g|w|t|f))?$'
        if not bool(re.match(patt,self.codeEdit.text())):
            self.parent().statusBar().showMessage("Code must match " + patt)
            self.codeEdit.setStyleSheet("color: rgb(255, 0, 0);")
            return False
        return True

    def logcql(self,string,cb):
        if cb.count() == 0:
            cb.addItem(string)
            print("logcql", cb.count(), cb.itemText(0))
            cb.setCurrentIndex(0)
        else:
            c = cb.count()
            cb.addItem(cb.itemText(c - 1))
            for i in range(c-1,0,-1):
                cb.setItemText(i,cb.itemText(i - 1))
            cb.setItemText(0,string)
            cb.setCurrentIndex(1)
        return True

    def cqlExec(self):
        if not self.cqlCheck():
            return
#        self.pb = QProgressBar(minimum = 0, maximum = 0)
#        self.pb.setVisible(True)
#        self.cqlvBox.addWidget(self.pb)
        self.logcql(self.cqlEdit.text(),self.cqlHistCombo)
        out = self.tsqh.translate(self.cqlEdit.text())
        q = hq.tssQuery(self.tssShelve,out,True,elindex=self.elindex)
        res, factor = q.execute()
        self.parent().statusBar().showMessage('Query klaar...')
        string = ""
        self.resTable.setRowCount(len(res))
        self.resTable.setVerticalHeaderLabels(r.hitString.id for r in res)
        for r,i in zip(res,range(len(res))):
            string += ' '.join(r.hitString.getwords()) + '\n'
            ident = r.hitString.id
            length = len(r.hitString)
            sent = self.tssShelve[ident]
            words = sent.getwords()
            item =  QTableWidgetItem(' '.join(str(i) for i in r.hitString.meta))
            self.resTable.setItem(i, 0, item)
            item =  QTableWidgetItem(' '.join(words[0:r.pos]))
            self.resTable.setItem(i, 1, item)
            item =  QTableWidgetItem(' '.join(r.hitString.getwords()))
            self.resTable.setItem(i, 2, item)
            item =  QTableWidgetItem(' '.join(words[length + r.pos:]))
            self.resTable.setItem(i, 3, item)
#            self.resBrowser.setText(string)
#        self.pb.setVisible(False)
#        self.resTable.resizeColumnsToContents()
        self.resTable.resizeRowsToContents()
        if len(res) == 0:
            self.parent().statusBar().showMessage('No fragments found')
        else:
            self.parent().statusBar().showMessage('Fetched {} fragments out of ca. {}'.format(str(len(res)),factor))

    def newCQLGroup(self):
        cp = self.cqlEdit.cursorPosition()
        ct = self.cqlEdit.text()
        nt = ct[:cp] + '[=""]' + ct[cp:]
        self.cqlEdit.setText(nt)
        self.cqlEdit.setCursorPosition(cp + 1)

    def newempCQLGroup(self):
        cp = self.cqlEdit.cursorPosition()
        ct = self.cqlEdit.text()
        nt = ct[:cp] + '[]{0,}' + ct[cp:]
        self.cqlEdit.setText(nt)
        self.cqlEdit.setCursorPosition(cp + 1)

    def saveExec(self):
        if not self.cqlCheck():
            return
        if not self.codeCheck():
            return
        f = open(self.savefile, mode='a', encoding='utf8')
        t = self.cqlEdit.text() + '\t' + self.codeEdit.text() + '\n'
        f.write(t)
        f.close()
        self.parent().statusBar().showMessage("Wrote to file: " + t)
        self.codeEdit.setText('')
        return

    def expExec(self):
        if self.resTable.rowCount() == 0:
            self.parent().statusBar().showMessage('Nothing to export')
            return
        df = self.tablewidgetToDF(self.resTable)
        filename, _ =  QFileDialog.getSaveFileName(self,'Select file for exporting query results',
                                                   "" if not self.expdir else self.expdir,
                                                   'CSV files (*.csv);;Tab delimited files (*.tsv)')
        self.expdir = os.path.dirname(filename)
        if not filename or filename == '':
            self.parent().statusBar().showMessage('You have to provide a filename')
            return
        df.to_csv(path_or_buf=filename,sep='\t',columns=['head','meta','pre','hit','post'],index=False)
        self.parent().statusBar().showMessage('Exported ' +  str(self.resTable.rowCount()) + ' rows in ' + filename)
        return
    
    def tablewidgetToDF(self, tb):
        l = []
        for i in range(tb.rowCount()):
            d = {}
            for j in range(tb.columnCount()):
                d[tb.horizontalHeaderItem(j).text()] = tb.item(i,j).text()
                d['head'] = tb.verticalHeaderItem(i).text()
            l.append(d)
        return pd.DataFrame(l)

