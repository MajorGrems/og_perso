import random
import time

import pyautogui as screen

x, y = screen.size()
while True:
    x1 = random.randint(1, 1)
    y1 = random.randint(1, 1)
    screen.moveTo(x1, y1)
    screen.click()
    time.sleep(30)
