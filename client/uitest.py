# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'uitest.ui'
#
# Created by: PyQt5 UI code generator 5.10.1
#
# WARNING! All changes made in this file will be lost!
from regpopup import Ui_Dialog
from client import Client, Message
from PyQt5 import QtCore, QtGui, QtWidgets
from threading import Thread

class Ui_MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        QtWidgets.QMainWindow.__init__(self)
        self.client = Client()
        self.dialog = Ui_Dialog(self.client)
        self.check_regitration()
        self.setupUi()

        incomm_thread = Thread(target= self.handle_incomming_messages, name= 'handle_incomming_messages in uitest')
        incomm_thread.setDaemon(True)
        incomm_thread.start()

    def closeEvent(self, event):
        self.client.save_client_state()
        event.accept()

    def setupUi(self):
        self.setObjectName("MainWindow")
        self.resize(793, 560)
        self.centralwidget = QtWidgets.QWidget(self)
        self.centralwidget.setObjectName("centralwidget")
        self.gridLayoutWidget = QtWidgets.QWidget(self.centralwidget)
        self.gridLayoutWidget.setGeometry(QtCore.QRect(20, 30, 751, 501))
        self.gridLayoutWidget.setObjectName("gridLayoutWidget")
        self.gridLayout = QtWidgets.QGridLayout(self.gridLayoutWidget)
        self.gridLayout.setContentsMargins(0, 0, 0, 0)
        self.gridLayout.setObjectName("gridLayout")
        self.scrollArea = QtWidgets.QScrollArea(self.gridLayoutWidget)
        self.scrollArea.setWidgetResizable(True)
        self.scrollArea.setObjectName("scrollArea")
        self.scrollAreaWidgetContents = QtWidgets.QWidget()
        self.scrollAreaWidgetContents.setGeometry(QtCore.QRect(0, 0, 747, 497))
        self.scrollAreaWidgetContents.setObjectName("scrollAreaWidgetContents")
        self.listWidget_contacts = QtWidgets.QListWidget(self.scrollAreaWidgetContents)
        self.listWidget_contacts.setGeometry(QtCore.QRect(20, 40, 151, 421))
        self.listWidget_contacts.setLineWidth(3)
        self.listWidget_contacts.setObjectName("listWidget_contacts")
        self.listWidget_2 = QtWidgets.QListWidget(self.scrollAreaWidgetContents)
        self.listWidget_2.setGeometry(QtCore.QRect(595, 250, 131, 211))
        #self.listWidget_2.setGeometry(QtCore.QRect(190, 80, 371, 331))
        self.listWidget_2.setObjectName("listWidget_2")
        self.textEdit = QtWidgets.QTextEdit(self.scrollAreaWidgetContents)
        self.textEdit.setGeometry(QtCore.QRect(197, 420, 321, 41))
        self.textEdit.setObjectName("textEdit")
        self.pushButton_send = QtWidgets.QPushButton(self.scrollAreaWidgetContents)
        self.pushButton_send.setGeometry(QtCore.QRect(527, 420, 41, 41))
        self.pushButton_send.setObjectName("pushButton_send")
        self.label = QtWidgets.QLabel(self.scrollAreaWidgetContents)
        self.label.setGeometry(QtCore.QRect(20, 10, 101, 31))
        self.label.setObjectName("label")
        self.label_2 = QtWidgets.QLabel(self.scrollAreaWidgetContents)
        self.label_2.setGeometry(QtCore.QRect(197, 25, 141, 41))
        self.label_2.setObjectName("label_2")
        self.pushButton_del_contact = QtWidgets.QPushButton(self.scrollAreaWidgetContents)
        self.pushButton_del_contact.setGeometry(QtCore.QRect(595, 120, 131, 31))
        #self.pushButton_del_contact.setGeometry(QtCore.QRect(600, 100, 131, 31))
        self.pushButton_del_contact.setObjectName("pushButton_del_contact")
        self.label_3 = QtWidgets.QLabel(self.scrollAreaWidgetContents)
        self.label_3.setGeometry(QtCore.QRect(595, 10, 91, 31))
        #self.label_3.setGeometry(QtCore.QRect(600, 10, 91, 31))
        self.label_3.setObjectName("label_3")
        self.lineEdit = QtWidgets.QLineEdit(self.scrollAreaWidgetContents)
        self.lineEdit.setGeometry(595, 210, 131, 25)
        self.lineEdit.setObjectName("search_input")
        self.pushButton_search_contact = QtWidgets.QPushButton(self.scrollAreaWidgetContents)
        self.pushButton_search_contact.setGeometry(QtCore.QRect(595, 170, 131, 31))
        #self.pushButton_search_contact.setGeometry(QtCore.QRect(600, 150, 131, 31))
        self.pushButton_search_contact.setObjectName("pushButton_search_contact")
        self.pushButton_add_contact = QtWidgets.QPushButton(self.scrollAreaWidgetContents)
        #self.pushButton_add_contact.setGeometry(QtCore.QRect(600, 50, 131, 31))
        self.pushButton_add_contact.setGeometry(QtCore.QRect(595, 90, 131, 31))
        self.pushButton_add_contact.setObjectName("pushButton_add_contact")
        # self.scrollArea_2 = QtWidgets.QScrollArea(self.scrollAreaWidgetContents)
        # self.scrollArea_2.setGeometry(QtCore.QRect(197, 69, 371, 341))
        # # self.scrollArea_2.setGeometry(QtCore.QRect(590, 200, 141, 211))
        # self.scrollArea_2.setWidgetResizable(True)
        # self.scrollArea_2.setObjectName("scrollArea_2")
        self.scrollAreaWidgetContents_2 = QtWidgets.QWidget()
        self.scrollAreaWidgetContents_2.setGeometry(QtCore.QRect(0, 0, 369, 339))
        #self.scrollAreaWidgetContents_2.setGeometry(QtCore.QRect(0, 0, 139, 209))
        self.scrollAreaWidgetContents_2.setObjectName("scrollAreaWidgetContents_2")
        # self.scrollArea_2.setWidget(self.scrollAreaWidgetContents_2)
        self.scrollArea.setWidget(self.scrollAreaWidgetContents)
        self.gridLayout.addWidget(self.scrollArea, 0, 0, 1, 1)
        self.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(self)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 775, 22))
        self.menubar.setObjectName("menubar")
        self.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(self)
        self.statusbar.setObjectName("statusbar")
        self.setStatusBar(self.statusbar)
        self.conversList = QtWidgets.QListWidget(self.scrollAreaWidgetContents)
        self.conversList.setObjectName("conversList")
        self.conversList.setGeometry(QtCore.QRect(197, 69, 371, 341))
        self.pushButton_register = QtWidgets.QPushButton(self.scrollAreaWidgetContents)
        self.pushButton_register.setGeometry(QtCore.QRect(595, 44, 131, 31))
        self.pushButton_register.setObjectName("pushButton_register")        
        
        self.set_contact_list()

        self.retranslateUi(self)
        QtCore.QMetaObject.connectSlotsByName(self)

        self.pushButton_add_contact.clicked.connect(self.button_add_contact_clicked)
        self.pushButton_del_contact.clicked.connect(self.button_del_contact_clicked)
        self.pushButton_search_contact.clicked.connect(self.button_search_contact_clicked)
        self.pushButton_send.clicked.connect(self.button_send_clicked)
        self.pushButton_register.clicked.connect(self.check_regitration)
        self.listWidget_contacts.clicked.connect(self.contact_list_item_selected)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "WhatsAppP2P"))
        self.pushButton_send.setText(_translate("MainWindow", "Send"))
        self.label.setText(_translate("MainWindow", "Contacts"))
        self.label_2.setText(_translate("MainWindow", "Contact_Name"))
        self.pushButton_del_contact.setText(_translate("MainWindow", "Delete Contact"))
        self.label_3.setText(_translate("MainWindow", "Options"))
        self.pushButton_search_contact.setText(_translate("MainWindow", "Search Contact"))
        self.pushButton_add_contact.setText(_translate("MainWindow", "Add Contact"))
        self.pushButton_register.setText(_translate("MainWindow", "Register"))

    def set_contact_list(self):
        for contact in self.client.contacts_info.keys():
            item = CustomListItem(contact, self.client.contacts_info[contact])  
            item.setText(item.contact_name)          
            self.listWidget_contacts.addItem(item)

    def button_add_contact_clicked(self):
        cont = self.listWidget_2.currentItem()
        if cont:
            newinfo = self.client.add_contact(cont.text())
            item = CustomListItem(cont.text(), newinfo)
            item.setText(item.contact_name)
            self.listWidget_contacts.addItem(item) 

    def button_del_contact_clicked(self):
        cont = self.listWidget_contacts.currentItem()
        if cont:
            self.client.delete_contact(cont.text())
            self.listWidget_contacts.removeItemWidget(cont)
            self.listWidget_contacts.repaint()

    def button_search_contact_clicked(self):
        result = self.client.search_contact(self.lineEdit.text())
        self.listWidget_2.clear()
        print(result)
        if result:
            item = CustomListItem(self.lineEdit.text(), result)
            item.setText(item.contact_name)
            self.listWidget_2.addItem(item)

            
            
    def contact_list_item_selected(self):
        item = self.listWidget_contacts.currentItem()
        print(item)
        self.client.active_user = item.contact_name
        self.label_2.setText(item.contact_name)
        print(item.contact_data['conversation'])
        self.show_conversation(item.contact_data['conversation'])
 


    def button_send_clicked(self):
        text = self.textEdit.toPlainText()
        target = self.listWidget_contacts.currentItem()
        if not target:
            target = self.listWidget_2.currentItem()
            if not target:
                return                
        if self.client.send_message_client(target.contact_name, text):
            item = CustomListItem(self.client.username, text)
            item.setText(f'{item.contact_name}\n {item.contact_data}')
            self.conversList.addItem(item)
            item.setTextAlignment(0x002)
            messg_list = target.contact_data['conversation']             
            if messg_list.count < self.conversList.count():
                self.conversList.takeItem(0)
        
        
    def check_regitration(self):
        if not self.client.registered:
            self.dialog.show()
        else:
            self.setWindowTitle("WhatsAppP2P--{0}".format(self.client.username))
        
        
    def show_conversation(self, messg_list):
        self.conversList.clear()
        if messg_list == None:
            return
        for mesg in messg_list:
            item = CustomListItem(mesg.sender, mesg.text)
            item.setText(f'{item.contact_name}\n {item.contact_data}')
            self.conversList.addItem(item) 
            if not mesg.sender == self.client.username:
                item.setTextAlignment(0x001)
            else:
                item.setTextAlignment(0x002)

    def handle_incomming_messages(self):
        queue = self.client.incomming_queue
        print(queue)
        while True:
            active_contact = self.listWidget_contacts.currentItem()
            while not queue.isEmpty:
                print('queue not empty')
                message = self.client.incomming_queue.pop() 
                sender = message.sender
                if active_contact == sender:
                    print('active contact is sender')
                    item = CustomListItem(sender, message.text)
                    item.setText(f'{item.contact_name}\n {item.contact_data}')
                    self.conversList.addItem(item)
                    item.setTextAlignment(0x001)   
                else:
                    print('sender is not active contact')
                    self.client.__log_message__(sender, message)
                    if not self.listWidget_contacts.findItems(sender, QtCore.Qt.MatchFixedString):
                        contact_to_add = CustomListItem(sender, self.client.contacts_info[sender])
                        contact_to_add.setText(sender)
                        self.listWidget_contacts.addItem(contact_to_add)
                    
                    


class CustomConversation(QtWidgets.QWidget):
    def __init__(self):
        QtWidgets.QWidget.__init__(self)
        self.items = []
        self.setLayout(QtWidgets.QVBoxLayout)

    def addItem(self, item):
        self.items.append(item)

class CustomListItem(QtWidgets.QListWidgetItem):
    def __init__(self, name, dictionary):
        QtWidgets.QListWidgetItem.__init__(self)
        self.contact_name = name
        self.contact_data = dictionary
    
    def text(self):
        return self.contact_name

    def item_info(self):
        return self.contact_data

    def item_info_specific(self, wanted: str):
        return self.contact_data[wanted]

    def __str__(self):
        return str({'name': self.contact_name, 'data': self.contact_data})

    def __repr__(self):
        return self.__str__()
