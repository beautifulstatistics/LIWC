from googletrans import Translator

samp = we['text_preprocess'].sample(n=20).values
trans = Translator()

tt = trans.translate(samp,dest='en',src='zh-cn')
tt.text