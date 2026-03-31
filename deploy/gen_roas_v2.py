"""
ROAS chart animation v2:
- Uses the actual rendered SVG as-is
- Progressive left-to-right reveal (line draws from 2X to 4X)
- 3 second draw, 3 second hold, 2 second fade-reset for loop
- Output at 340x250 to match bloom-overlay
"""
import os, math, shutil, tempfile, subprocess
from PIL import Image, ImageDraw, ImageFilter

SRC = "/tmp/roas-big.png"
OUT = "/Users/zohaahmed/Downloads/Claude/deploy/assets/roas-chart.mp4"
FPS = 30
W_OUT, H_OUT = 340, 250

# Load the full rendered chart
full = Image.open(SRC).convert("RGBA")
W, H = full.size

# Create a version with just the left bloom (mask everything right of ~30%)
# And progressive reveals

# The SVG layout (from reading it):
# Left bloom center: x=60/300 * W = 120px at 600w
# Right bloom center: x=240/300 * W = 480px at 600w
# Line goes from ~120,222 to ~480,135 (at 600w scale)

LEFT_X = int(60 / 300 * W)   # ~120
RIGHT_X = int(240 / 300 * W) # ~480

# Phases:
# 0-3s (0-90 frames): reveal from left to right
# 3-6s (90-180): hold full chart
# 6-8s (180-240): gentle pulse then soft reset

DRAW_FRAMES = 90   # 3 seconds
HOLD_FRAMES = 90   # 3 seconds
FADE_FRAMES = 60   # 2 seconds
TOTAL = DRAW_FRAMES + HOLD_FRAMES + FADE_FRAMES  # 240 = 8s

def ease_out_cubic(t):
    return 1 - (1 - t) ** 3

tmpdir = tempfile.mkdtemp()
print(f"Generating {TOTAL} frames...")

# Pre-make a background (the leftmost part of the chart, just the 2X bloom area)
bg_color = (255, 253, 248, 255)

for i in range(TOTAL):
    frame = Image.new("RGBA", (W, H), bg_color)

    if i < DRAW_FRAMES:
        # Drawing phase: progressive reveal left to right
        t = ease_out_cubic(i / DRAW_FRAMES)
        # Reveal x goes from just past left bloom to full width
        reveal_start = LEFT_X + 40  # start just past the left bloom
        reveal_x = int(reveal_start + (W - reveal_start) * t)

        # Always show the left bloom area
        left_crop = full.crop((0, 0, LEFT_X + 40, H))
        frame.paste(left_crop, (0, 0), left_crop)

        # Progressive reveal of the rest with soft edge
        if reveal_x > LEFT_X + 40:
            reveal_crop = full.crop((LEFT_X + 40, 0, reveal_x, H))
            frame.paste(reveal_crop, (LEFT_X + 40, 0), reveal_crop)

            # Soft leading edge: fade the rightmost ~30px
            edge_width = min(30, reveal_x - LEFT_X - 40)
            if edge_width > 2:
                for ex in range(edge_width):
                    alpha = int(255 * (1 - ex / edge_width))
                    col_x = reveal_x - ex - 1
                    if 0 <= col_x < W:
                        for ey in range(H):
                            px = frame.getpixel((col_x, ey))
                            if px[3] > 0:
                                # Blend with background
                                blend = int(px[3] * (1 - ex / edge_width))
                                frame.putpixel((col_x, ey), (px[0], px[1], px[2], max(0, blend)))

    elif i < DRAW_FRAMES + HOLD_FRAMES:
        # Hold phase: full chart visible with gentle breathing
        breath_t = (i - DRAW_FRAMES) / HOLD_FRAMES
        scale = 1.0 + 0.008 * math.sin(breath_t * math.pi * 2)

        if abs(scale - 1.0) > 0.001:
            sw = int(W * scale)
            sh = int(H * scale)
            scaled = full.resize((sw, sh), Image.LANCZOS)
            ox = (sw - W) // 2
            oy = (sh - H) // 2
            cropped = scaled.crop((ox, oy, ox + W, oy + H))
            frame.paste(cropped, (0, 0), cropped)
        else:
            frame.paste(full, (0, 0), full)

    else:
        # Fade phase: hold full, then gently fade and restart
        fade_t = (i - DRAW_FRAMES - HOLD_FRAMES) / FADE_FRAMES

        if fade_t < 0.5:
            # First half: still showing full
            frame.paste(full, (0, 0), full)
        else:
            # Second half: fade out
            alpha_mult = 1.0 - (fade_t - 0.5) * 2
            faded = full.copy()
            # Apply alpha
            r, g, b, a = faded.split()
            a = a.point(lambda p: int(p * alpha_mult))
            faded = Image.merge("RGBA", (r, g, b, a))
            frame.paste(faded, (0, 0), faded)

    # Flatten to RGB on bg color
    flat = Image.new("RGB", (W, H), bg_color[:3])
    flat.paste(frame, (0, 0), frame)

    # Resize to output
    final = flat.resize((W_OUT, H_OUT), Image.LANCZOS)
    final.save(os.path.join(tmpdir, f"frame_{i:04d}.png"))

    if i % 60 == 0:
        print(f"  Frame {i}/{TOTAL}")

print("Encoding video...")
subprocess.run([
    "ffmpeg", "-y",
    "-framerate", str(FPS),
    "-i", os.path.join(tmpdir, "frame_%04d.png"),
    "-c:v", "libx264",
    "-pix_fmt", "yuv420p",
    "-crf", "20",
    "-preset", "medium",
    "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
    OUT
], check=True, capture_output=True)

shutil.rmtree(tmpdir)
print(f"Done! {OUT}")
print(f"Size: {os.path.getsize(OUT) / 1024:.1f} KB")
