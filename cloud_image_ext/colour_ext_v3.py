import extcolors
import PIL

class Image_Colour_Extract(object):
	def __init__(self, image):
		self.image = image
		self.final_colours = ['gray', 'dark_blue', 'white', 'black', 'light_blue', 'brown', 'light_brown', 'orange', 'pink']
		self.colors = None
		self.pixel_count = None
		self.dom_col = list()
		self.final_colours_dict =  {'gray':[128,128,128], 'dark_blue':[0,0,200], 'white':[255,255,255], 'black':[0,0,0], 'light_blue':[0,0,255], 'brown':[102,51,0], 'light_brown':[204,102,0], 'orange':[255,128,0], 'pink':[255,204,255]}
	
	def value_intake(self):
		self.colors, self.pixel_count = extcolors.extract_from_image(self.image)

	def percent_colors(self):
		self.value_intake()
		color_percent = dict()
		for i in self.colors:
			color_percent[i[0]] = (i[1]/self.pixel_count)*100
			self.dom_col.append(list(i[0]))
	
	def mandist(self, vector1,vector2):
		return sum(map(lambda v1, v2: abs(v1 - v2), vector1, vector2))
	
	def perc_dict(self,dict1):
		dict_1 = dict()
		for i in dict1.items():
			dict_1[i[0]] = (i[1]/self.pixel_count)*100
		return dict_1
	
	def process(self, color_closest):
		color_pixel = dict()
		for i in color_closest:
			if i[1] in color_pixel.keys():
				color_pixel[i[1]] += i[3]

			else:
				color_pixel[i[1]] = i[3]
		return color_pixel
	
	
	def nearestNeighbour(self):
		self.percent_colors()
		nearest_color = []
		for i in self.dom_col:
			temp = []
			for j in self.final_colours_dict.items():
				temp.append([self.mandist(j[1],i),j[0]])
			min_dist = min(temp)
			min_dist.append(i)
			nearest_color.append(min_dist)
			
		return nearest_color


	def NNperc(self):
		near_col = self.nearestNeighbour()
		perc_dict = dict()
		final_colours_dict1 = dict()
		for i in range(len(near_col)):
			near_col[i].append(self.colors[i][1])	
		perc_dict1 = self.process(near_col)		
		new_dict = self.perc_dict(perc_dict1)
		return new_dict
	


