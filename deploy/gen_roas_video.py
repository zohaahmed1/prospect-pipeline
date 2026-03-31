"""
Generate a slow-moving ROAS growth animation video.
- Line draws from 2X to 4X over time
- Bloom flowers grow at start and end
- Labels fade in
- Smooth, loopable, 8 seconds at 30fps
- Output: 340x250 mp4 (matches bloom-overlay size)
"""
import math, os, subprocess, shutil, tempfile
from PIL import Image, ImageDraw, ImageFont

W, H = 680, 500  # 2x for crisp, will scale down
FPS = 30
DURATION = 8  # seconds
TOTAL_FRAMES = FPS * DURATION

# Colors
BG = (250, 249, 247, 0)  # transparent-ish, we'll use white
BG_SOLID = (255, 253, 248)
PURPLE = (85, 0, 221)
TEAL = (0, 180, 160)
DARK = (25, 25, 35)
GRAY = (168, 168, 168)
WHITE = (255, 255, 255)

# Chart coords (in 2x space)
CHART_LEFT = 100
CHART_RIGHT = 580
CHART_TOP = 60
CHART_BOTTOM = 340
LABEL_Y = 380

# Easing
def ease_out_cubic(t):
    return 1 - (1 - t) ** 3

def ease_in_out(t):
    return 3 * t * t - 2 * t * t * t

def lerp(a, b, t):
    return a + (b - a) * t

def draw_bloom(draw, cx, cy, radius, color, petals, progress, opacity=1.0):
    """Draw a flower bloom with animated growth."""
    r = int(radius * progress)
    if r < 2:
        return

    # Outer glow
    glow_r = int(r * 1.8)
    glow_color = (*color, int(15 * opacity))
    draw.ellipse([cx - glow_r, cy - glow_r, cx + glow_r, cy + glow_r], fill=glow_color)

    # Petals (outer ring)
    for i in range(petals):
        angle = (2 * math.pi * i / petals) + math.pi / 6
        pr = r * 0.9
        px = cx + math.cos(angle) * pr * 0.5
        py = cy + math.sin(angle) * pr * 0.5
        petal_r = pr * 0.45
        petal_color = (*color, int(140 * opacity * progress))
        draw.ellipse([px - petal_r, py - petal_r, px + petal_r, py + petal_r], fill=petal_color)

    # Inner petals
    for i in range(petals):
        angle = (2 * math.pi * i / petals)
        pr = r * 0.55
        px = cx + math.cos(angle) * pr * 0.4
        py = cy + math.sin(angle) * pr * 0.4
        petal_r = pr * 0.35
        petal_color = (*color, int(200 * opacity * progress))
        draw.ellipse([px - petal_r, py - petal_r, px + petal_r, py + petal_r], fill=petal_color)

    # Center
    center_r = int(r * 0.22)
    draw.ellipse([cx - center_r, cy - center_r, cx + center_r, cy + center_r], fill=(*DARK, int(180 * opacity)))
    inner_r = int(r * 0.18)
    draw.ellipse([cx - inner_r, cy - inner_r, cx + inner_r, cy + inner_r], fill=(*color, int(220 * opacity)))
    # Highlight
    hr = int(r * 0.06)
    hx, hy = cx - r * 0.05, cy - r * 0.05
    draw.ellipse([hx - hr, hy - hr, hx + hr, hy + hr], fill=(*WHITE, int(100 * opacity)))

def draw_label_pill(draw, cx, cy, text, border_color, progress, font):
    """Draw a rounded pill label."""
    if progress < 0.01:
        return
    alpha = int(255 * min(1, progress))
    pw, ph = 56, 28
    x0, y0 = cx - pw, cy - ph // 2
    x1, y1 = cx + pw, cy + ph // 2
    # White fill
    draw.rounded_rectangle([x0, y0, x1, y1], radius=ph // 2, fill=(*WHITE, alpha), outline=(*border_color, alpha), width=2)
    # Text
    bbox = font.getbbox(text)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    draw.text((cx - tw // 2, cy - th // 2 - 1), text, fill=(*DARK, alpha), font=font)

def generate_frame(frame_num):
    t = frame_num / TOTAL_FRAMES  # 0 to 1

    img = Image.new("RGBA", (W, H), (*BG_SOLID, 255))
    draw = ImageDraw.Draw(img)

    # Try to load a good font
    try:
        font_big = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 28)
        font_sm = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 18)
        font_label = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 22)
    except:
        font_big = ImageFont.load_default()
        font_sm = font_big
        font_label = font_big

    # Timeline phases
    # 0.0 - 0.15: left bloom grows
    # 0.1 - 0.7:  line draws across
    # 0.5 - 0.85: right bloom grows
    # 0.3 - 0.9:  labels fade in
    # 0.7 - 1.0:  subtle pulse/breathe

    # Small leaves along the line (grow as line passes)
    line_progress = ease_out_cubic(max(0, min(1, (t - 0.1) / 0.6)))

    # Start and end Y positions
    start_x, start_y = CHART_LEFT + 20, CHART_BOTTOM - 80
    end_x, end_y = CHART_RIGHT - 20, CHART_TOP + 60

    # Draw the line (growing)
    if line_progress > 0:
        cur_x = lerp(start_x, end_x, line_progress)
        cur_y = lerp(start_y, end_y, line_progress)

        # Thicker green line
        draw.line([(start_x, start_y), (cur_x, cur_y)], fill=(*TEAL, 60), width=6)
        draw.line([(start_x, start_y), (cur_x, cur_y)], fill=(42, 110, 55, 180), width=4)

        # Small leaves along the line
        for i in range(3):
            leaf_t = (i + 1) / 4
            if line_progress > leaf_t + 0.05:
                lx = lerp(start_x, end_x, leaf_t)
                ly = lerp(start_y, end_y, leaf_t)
                leaf_prog = min(1, (line_progress - leaf_t - 0.05) / 0.15)
                leaf_size = 8 * leaf_prog
                angle = -0.5 + i * 0.3
                lx2 = lx + math.cos(angle) * leaf_size * 2
                ly2 = ly + math.sin(angle) * leaf_size * 2
                draw.ellipse([lx2 - leaf_size, ly2 - leaf_size, lx2 + leaf_size, ly2 + leaf_size],
                           fill=(74, 160, 74, int(150 * leaf_prog)))

    # Left bloom (teal, 2X, 2025)
    bloom1_progress = ease_out_cubic(max(0, min(1, t / 0.2)))
    # Gentle breathing after grown
    if bloom1_progress >= 1:
        breath = 1 + 0.03 * math.sin(t * math.pi * 4)
        bloom1_progress = breath
    draw_bloom(draw, start_x, start_y, 65, TEAL, 6, min(1, bloom1_progress))

    # Right bloom (purple, 4X, 2026)
    bloom2_progress = ease_out_cubic(max(0, min(1, (t - 0.5) / 0.3)))
    if bloom2_progress >= 1:
        breath = 1 + 0.03 * math.sin(t * math.pi * 4 + 1)
        bloom2_progress = breath
    draw_bloom(draw, end_x, end_y, 80, PURPLE, 8, min(1, bloom2_progress))

    # Labels
    label1_prog = ease_in_out(max(0, min(1, (t - 0.15) / 0.15)))
    label2_prog = ease_in_out(max(0, min(1, (t - 0.7) / 0.15)))

    draw_label_pill(draw, start_x, start_y - 75, "2x", TEAL, label1_prog, font_big)
    draw_label_pill(draw, end_x, end_y - 85, "4x", PURPLE, label2_prog, font_big)

    # Year labels
    if label1_prog > 0:
        alpha = int(180 * label1_prog)
        draw.text((start_x - 15, start_y + 72), "2025", fill=(*TEAL, alpha), font=font_sm)
    if label2_prog > 0:
        alpha = int(180 * label2_prog)
        draw.text((end_x - 15, end_y + 88), "2026", fill=(*PURPLE, alpha), font=font_sm)

    # "ROAS GROWTH" label at bottom
    title_prog = ease_in_out(max(0, min(1, (t - 0.75) / 0.15)))
    if title_prog > 0:
        alpha = int(180 * title_prog)
        text = "ROAS GROWTH"
        bbox = font_label.getbbox(text)
        tw = bbox[2] - bbox[0]
        draw.text((W // 2 - tw // 2, H - 60), text, fill=(*PURPLE, alpha), font=font_label)

    # Scale down to target size
    final = img.resize((340, 250), Image.LANCZOS)
    return final.convert("RGB")

# Generate frames
tmpdir = tempfile.mkdtemp()
print(f"Generating {TOTAL_FRAMES} frames...")

for i in range(TOTAL_FRAMES):
    frame = generate_frame(i)
    frame.save(os.path.join(tmpdir, f"frame_{i:04d}.png"))
    if i % 30 == 0:
        print(f"  Frame {i}/{TOTAL_FRAMES}")

print("Encoding video...")
output_path = "/Users/zohaahmed/Downloads/Claude/deploy/assets/roas-chart.mp4"
subprocess.run([
    "ffmpeg", "-y",
    "-framerate", str(FPS),
    "-i", os.path.join(tmpdir, "frame_%04d.png"),
    "-c:v", "libx264",
    "-pix_fmt", "yuv420p",
    "-crf", "23",
    "-preset", "medium",
    "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
    output_path
], check=True, capture_output=True)

# Clean up
shutil.rmtree(tmpdir)
print(f"Done! Video saved to {output_path}")
print(f"Size: {os.path.getsize(output_path) / 1024:.1f} KB")
