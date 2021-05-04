import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import glob
from sklearn.cluster import KMeans
from collections import Counter
import cv2
import sklearn

class Image_Colour_Extract(object):
    def __init__(self, img):
        self.img = img
        self.mod_img = None
        self.list_img = list()
        self.list_perc = list()
        self.list_color = list()

    def RGB2HEX(self,color):
        return "#{:02x}{:02x}{:02x}".format(int(color[0]), int(color[1]), int(color[2]))

    def func(self,pct,values):
        return round(pct,3)
    
    def img_preprocessing(self):
        img1 = cv2.imread(self.img)
        img1 = cv2.cvtColor(img1,cv2.COLOR_BGR2RGB)
        self.mod_img = cv2.resize(img1,(600,400), interpolation = cv2.INTER_AREA)
        self.mod_img = mod_img.reshape(mod_img.shape[0]*mod_img.shape[1], 3)
        
    def clusters(self):
        self.img_preprocessing()
        list_img, list_perc = list(), list()
        clf = KMeans(n_clusters = 8)
        
        labels = clf.fit_predict(self.mod_img)
        center_colors = clf.cluster_centers_
        
        counts = Counter(labels)
        ordered_colors = [center_colors[i] for i in counts.keys()]
        hex_colors = [self.RGB2HEX(ordered_colors[i]) for i in counts.keys()]
        rgb_colors = [ordered_colors[i] for i in counts.keys()]
        
        plt.figure(figsize = (8, 6))
        plt.pie(counts.values(), labels = rgb_colors, colors = hex_colors, autopct = lambda pct: self.list_perc.append(self.func(pct, counts.values())))
        
        self.list_img.append(rgb_colors)

    def rgb_vals(self): 
        self.clusters()       
        for i in self.list_img:
            for j in i:
                j = j.astype(int)
                self.list_color.append(j)

    def dict_values(self):
        self.rgb_vals()
        final_dict = dict()

        for i in range(len(self.list_color)):
            for j in range(len(self.list_color[i])):
                final_dict[self.list_perc[j]] = list(self.list_color[j])

        return final_dict

    def color_list(self):
        self.dict_values()
        final_dict = self.dict_values()
        l_gray = [0]
        l_dark_blue = [0]
        l_white = [0]
        l_black = [0]
        l_light_blue = [0]
        l_brown = [0]
        l_light_brown = [0]
        l_orange = [0]
        l_pink = [0]

        for i in final_dict.values():
            if int(np.mean(i)) in range(i[0]-20,i[0]+20) and int(np.mean(i)) in range(i[1]-20,i[1]+20) and int(np.mean(i)) in range(i[2]-20,i[2]+20):
                if i[0] in range(100,200) or i[1] in range(100,200) or i[2] in range(100,200):
                    l_gray.append(list(final_dict.keys())[list(final_dict.values()).index(i)])
                elif i[0] in range (200,225) or i[1] in range(200,255) or i[2] in range(200,255):
                    l_white.append(list(final_dict.keys())[list(final_dict.values()).index(i)])
                elif i[0] in range(0,100) or i[1] in range(0,100) or i[2] in range(0,100):
                    l_black.append(list(final_dict.keys())[list(final_dict.values()).index(i)])
            else:
                if max(i)==i[2]:
                    if max(i) in range(0,180) and i[1]<150:
                        l_dark_blue.append(list(final_dict.keys())[list(final_dict.values()).index(i)])
                    else:
                        l_light_blue.append(list(final_dict.keys())[list(final_dict.values()).index(i)])

                elif max(i)==i[0]:
                    if i[0]<=100:
                        l_brown.append(list(final_dict.keys())[list(final_dict.values()).index(i)])
                    elif i[0] in range(101,200):
                        l_light_brown.append(list(final_dict.keys())[list(final_dict.values()).index(i)])
                    elif i[0] in range(200,255) and i[1]>100:
                        l_orange.append(list(final_dict.keys())[list(final_dict.values()).index(i)])
                    else:
                        l_pink.append(list(final_dict.keys())[list(final_dict.values()).index(i)])

        return l_black,l_gray,l_light_blue,l_dark_blue,l_white,l_brown,l_light_brown,l_orange,l_pink
    
    def percent_values(self):
        color_pct_dict = dict()
        l_black,l_gray,l_light_blue,l_dark_blue,l_white,l_brown,l_light_brown,l_orange,l_pink = self.color_list()
        color_pct_dict['black'] = np.mean(l_black)
        color_pct_dict['gray'] = np.mean(l_gray)
        color_pct_dict['light_blue'] = np.mean(l_light_blue)
        color_pct_dict['dark_blue'] = np.mean(l_dark_blue)
        color_pct_dict['white'] = np.mean(l_white)
        color_pct_dict['brown'] = np.mean(l_brown)
        color_pct_dict['light_brown'] = np.mean(l_light_brown)
        color_pct_dict['orange'] = np.mean(l_orange)
        color_pct_dict['pink'] = np.mean(l_pink)
        return color_pct_dict
    

