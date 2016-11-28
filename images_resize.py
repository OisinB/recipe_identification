# 
# Sample usage
# python prepare_images.py ../photos/luciano_photos ../photos/test 100
# 
# 

import os, sys
from PIL import Image
from math import floor

# newFolder = '../photos/luciano_small_photos/'
# imageFolder = '../photos/luciano_photos/'

def create_thumbnail(filePath, size):

  img = Image.open(filePath)
  img = img.copy()

  # if img.mode not in ('L', 'RGB'):
  #   img = img.convert('RGB')

  width, height = img.size

  if width < height:
    ratio = float(width) / float(height)
    newwidth = ratio * size
    img = img.resize((int(floor(newwidth)), size), Image.ANTIALIAS)

  elif width > height:
    ratio = float(height) / float(width)
    newheight = ratio * size
    img = img.resize((size, int(floor(newheight))), Image.ANTIALIAS)

  return img


def prepare_images(sourceFolder, destinationFolder, imageQuality):
  if not os.path.exists(destinationFolder):
    os.makedirs(destinationFolder)
  
  for file in os.listdir(sourceFolder):
    if file:
      im = create_thumbnail(sourceFolder + '/' + file, 320)
      # im.save(destinationFolder + '/' + file, "JPEG", quality=imageQuality)
      im.save(destinationFolder + '/' + file, "JPEG")

      
prepare_images(sys.argv[1], sys.argv[2], sys.argv[3])
