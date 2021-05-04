import extcolors
import PIL

img = PIL.Image.open("Ac-N001.jpg")
colors, pixel_count = extcolors.extract_from_image(img)
#print(colors, pixel_count)
final_colours = ['gray','dark_blue','white','black','light_blue','brown','light_brown','orange','pink']
dom_col = []
final_colours_dict = {'gray':[128,128,128],'dark_blue':[0,0,200],'white':[255,255,255],'black':[0,0,0],'light_blue':[0,0,255],'brown':[102,51,0],'light_brown':[204,102,0],'orange':[255,128,0],'pink':[255,204,255]}

def perc(colors1,pixel_count1):
	dict_1 = dict()
	#print(colors1)
	for i in colors1:
		#print(i)
		dict_1[i[0]] = (i[1]/pixel_count1)*100
		dom_col.append(list(i[0]))
	return dict_1 

def perc_dict(dict1):
	dict_1 = dict()
	for i in dict1.items():
		dict_1[i[0]] = (i[1]/pixel_count)*100
	return dict_1

def mandist(vector1,vector2):
	return sum(map(lambda v1, v2: abs(v1 - v2), vector1, vector2))
    
def nearestNeighbour(itemVector):
	t = []
	m = []
	#print([type(item[1])for item in final_colours_dict.items()])
	for i in itemVector:
		for j in final_colours_dict.items():
			t.append([mandist(j[1],i),j[0]])
		q = min(t)
		q.append(i)
		m.append(q)
		t = []
	#print(m)
	NNperc(m)
	return m
	#min([ (mandist(i, item[1]), item[0]) for i in itemVector for item in final_colours_dict.items()])

def process(ugh):
	colourss = []
	c=-1
	fuckthis = dict()
	for i in ugh:
		#print(i[1])
		if i[1] in fuckthis.keys():
			fuckthis[i[1]] += i[3]
			colourss
		else:
			fuckthis[i[1]] = i[3]
			colourss.append(i[1])
	return fuckthis

def NNperc(m):
	fuckthis = dict()
	ugh = []
	#dom_col = []
	final_colours_dict1 = dict()
	for i in range(len(m)):
		m[i].append(colors[i][1])	
	for i in m:
		ugh.append([i[1],i[3]])
	#print(ugh)
	fuckthis = process(m)		
	#print(fuckthis)
	#new_dict = perc(ugh,pixel_count)
	new_dict = perc_dict(fuckthis)
	print(new_dict)
	



#perc(colors,pixel_count)
print(perc(colors,pixel_count))
#print(type(dom_col[0]))
nearestNeighbour(dom_col)
