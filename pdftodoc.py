def education_details():
   import pdf2docx
   from pdf2docx import Converter
   import os
   from sklearn.datasets import load_iris

   path_input = r'C:\Users\AbhigyanSS\Desktop\Aurthbridge\download\pdf\\'
   path_output = r'C:\Users\AbhigyanSS\Desktop\Aurthbridge\download\text\\'
   os.chdir(path_input)
   for file in os.listdir(path_input):
      cv = Converter(path_input + file)
      cv.convert(path_output + file + '.docx', start=0, end=None)
      cv.close()
      print(file)

   # save to csv
   from docx import Document
   import pandas as pd
   document = Document(f"C:\\Users\\AbhigyanSS\\Desktop\\Aurthbridge\\download\\text\\{file}.docx")

   tables = []
   for table in document.tables:
      df = [['' for i in range(len(table.columns))] for j in range(len(table.rows))]
      for i, row in enumerate(table.rows):
         for j, cell in enumerate(row.cells):
            if cell.text:
               df[i][j] = cell.text
      tables.append(pd.DataFrame(df))

   for nr, i in enumerate(tables):
      i.to_csv("table_" + str(nr) + ".csv")
   document.save(f"C:\\Users\\AbhigyanSS\\Desktop\\Aurthbridge\\download\\text\\{file}.docx")