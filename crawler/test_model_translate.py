import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

model_name = "VietAI/envit5-translation"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

# chọn device
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = model.to(device)

texts = [
    "vi: VietAI là tổ chức phi lợi nhuận ...",
    "vi: Theo báo cáo mới nhất của Linkedin ...",
    "en: Our teams aspire to make discoveries ...",
    "en: We're on a journey to advance ..."
]

texts = [
    model_vi2en = AutoModelForSeq2SeqLM.from_pretrained("vinai/vinai-translate-vi2en-v2")
]

# tokenize và chuyển cả batch lên cùng device
batch = tokenizer(texts, return_tensors="pt", padding=True).to(device)

# generate
outputs = model.generate(**batch, max_length=512)

# decode (chuyển kết quả về CPU để decode)
decoded = tokenizer.batch_decode(outputs.cpu(), skip_special_tokens=True)
print(decoded)
