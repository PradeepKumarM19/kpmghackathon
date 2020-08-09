# import all functions from the tkinter   
from tkinter import *
import spacy
import en_core_web_lg
import argparse
#from information_retrival_v1 import get_anonymized_text

# Function to clear both the text areas 
def clearAll() : 
    # whole content of text area  is deleted  
    text1_field.delete(1.0, END) 
    text2_field.delete(1.0, END) 


def get_anonymized_text():

    input_text = text1_field.get("1.0", "end")[:-1] 
    nlp = en_core_web_lg.load()
    #Parse the text using spaCy
    doc = nlp(input_text)
    #Check Named entity Recognition
    for token in doc:
        token, token.ent_type_
    
    #For replacing the person names from the given text.
    anonymized_text = remove_names_persons(nlp,input_text)
    text2_field.insert('end -1 chars', anonymized_text)


def remove_names_persons(nlp_lang,text):
    #Parse the text
    doc = nlp_lang(text)
    #Updated document
    updated_doc = []
    person_replacement_token = ' <Name> '

    #Check Entity type
    for token in doc:
        if token.ent_type_ == 'ORG':
            updated_doc.append(' <Orginazation> ')
        elif token.ent_type_ == 'NORP':
            updated_doc.append(' <Nationalities> ')
        elif token.ent_type_ == 'GPE':
            updated_doc.append(' <Location> ')
        elif token.ent_type_ == 'LANGUAGE':
            updated_doc.append(' <LANGUAGE> ')
        elif token.ent_type_ == 'PERSON':
            if token.ent_iob_ == 'B':
                #Replace starting entity word
                updated_doc.append(person_replacement_token)
            else:
                #ignore 
                pass
        else:
            updated_doc.append(token.string)
    #print("output text",updated_doc)
    return ''.join(updated_doc)

# Driver code  
if __name__ == "__main__" :  
    # Create a GUI window  
    root = Tk() 
  
    # Set the background colour of GUI window,SystemButtonFace   
    root.configure(background = 'khaki2')   
    root.title("Fee Fie Foe Fum")
    frame=Frame(root, width=600, height=320)
    #self.canvas = Canvas(self.tk, width = 500, height = 500)
      
    # Set the configuration of GUI window (WidthxHeight)  
    root.geometry("400x350")   

    # set the name of tkinter GUI window   
    root.title("Anonymized Data") 
      
    # Create Welcome to Morse Code Translator label   
    headlabel = Label(root, text = 'Welcome to Anonymized text converter',   
                      fg = 'black')   
    # the widgets at respective positions in table like structure .    
    headlabel.grid(row = 0, column = 1)  
       # Create a "Text " label   
    #Label(root, text = "Enter Num 2:").grid(row=1)
     # grid method is used for placing   
  
        
    # Create a text area box   
    # for filling or typing the information.   
    text1_field = Text(root, height = 5, width = 40, font = "lucida 13")  
    text2_field = Text(root, height = 5, width = 40, font = "lucida 13") 
    #text.pack(expand=True, fill=BOTH)
         
    # padx keyword argument used to set paading along x-axis . 
    # pady keyword argument used to set paading along y-axis .   
    text1_field.grid(row = 1, column = 1, padx = 10, pady = 10)   
    text2_field.grid(row = 3, column = 1, padx = 10, pady = 10)  
  
        
    # Create a Convert Button and attached   
    # with convert function   
    input_text = text1_field.get("1.0", "end")[:-1]
    print(input_text)
    button1 = Button(root, text = "Convert Anonymized Text",  fg = "black",  
                                command = get_anonymized_text ) 
        
    button1.grid(row = 2, column = 1)  
    
    # Create a Clear Button and attached   
    # with clearAll function   
    button2 = Button(root, text = "Clear",   
                     fg = "black", command = clearAll) 
      
    button2.grid(row = 4, column = 1)  
      
    # Start the GUI   
    root.mainloop()