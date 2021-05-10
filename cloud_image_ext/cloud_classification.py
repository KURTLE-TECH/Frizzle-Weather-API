import tensorflow.keras
from PIL import Image, ImageOps
import numpy as np


class Cloud_Classification(object):
    def __init__(self, image):
        np.set_printoptions(suppress = True)
        self.model = tensorflow.keras.models.load_model('keras_model.h5')
        self.data = np.ndarray(shape = (1, 224, 224, 3), dtype = np.float32) 
        self.image = Image.open(image)

    def preprocessing(self):
        size = (224, 224)
        resize_image = ImageOps.fit(self.image, size, Image.ANTIALIAS)
        image_array = np.asarray(resize_image)
        normalized_image_array = (image_array.astype(np.float32) / 127.0) - 1
        self.data[0] = normalized_image_array

    def cloud_classify(self):
        prediction = self.model.predict(self.data)
        return prediction