from PIL import Image, ImageDraw, ImageFont
import traceback

print('start')
img = Image.new('RGB', (360,120), (255,255,255))
d = ImageDraw.Draw(img)
d.rounded_rectangle([40, 42, 250, 90], radius=12, fill=(18,32,62))
print('rect ok')
try:
    try:
        font = ImageFont.truetype('arial.ttf', 28)
        print('truetype ok')
    except Exception as e:
        print('truetype failed', e)
        font = ImageFont.load_default()
        print('default font ok')

    d.text((20,8), 'NOVA MAX', font=font, fill=(18,32,62))
    print('text ok')
except Exception:
    traceback.print_exc()
    raise

img.save('..\\store-panel\\public\\logo.png')
img.save('..\\driver-panel\\public\\logo.png')
print('saved')
