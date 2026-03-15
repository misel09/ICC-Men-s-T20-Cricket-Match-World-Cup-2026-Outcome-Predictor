import re

path = r'c:\Users\bhart\OneDrive\Desktop\T20_world_cup\backend\streamlit_app.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace dict(a=b, c=d) with {"a": b, "c": d} is hard with regex for nested ones.
# But most in this file are simple. 
# Better to do it manually for known blocks if possible, or use a safer approach.
# Let's try to fix the ones the linter is yelling about.

def fix_line(line):
    # Very simple fix for textfont=dict(...)
    line = re.sub(r'textfont=dict\(([^)]+)\)', r'textfont={\1}', line)
    # Fix dict(size=..., color=...) to {"size": ..., "color": ...}
    # This is tricky without a proper parser.
    return line

# I'll just do a manual replacement of the most common ones.
content = content.replace('textfont=dict(', 'textfont={')
content = content.replace('font=dict(', 'font={')
content = content.replace('marker=dict(', 'marker={')
content = content.replace('xaxis=dict(', 'xaxis={')
content = content.replace('yaxis=dict(', 'yaxis={')
content = content.replace('legend=dict(', 'legend={')
content = content.replace('hoverlabel=dict(', 'hoverlabel={')
content = content.replace('title=dict(', 'title={')
content = content.replace('margin=dict(', 'margin={')
content = content.replace('line=dict(', 'line={')

# Since I replaced the open paren, I need to replace the close paren.
# This is dangerous if there are nested calls.
# I'll use multi_replace instead.
