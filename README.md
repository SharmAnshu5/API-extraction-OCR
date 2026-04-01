RO_OCR_DATA


```
 git init
>> git add README.md
>> git commit -m "first commit"
>> git branch -M main
>> git remote add origin https://github.com/utkarshsharma2002us/API-extraction-OCR.git
>> git push -u origin main



git pull 
git add .
git commit -m "Product"
git push -u origin main
```



📁 Multiple Folder Watch
        ↓
🆔 Identifier (file type / source detection)
        ↓
🔀 Router (decide processing logic)
        ↓
📄 Parser (extract text using OpenAI / OCR)
        ↓
✅ Validation (business rules + data checks)
        ↓
❌ If Error → 📧 Send Email Alert
        ↓
▶️ main.py (final processing & storage)