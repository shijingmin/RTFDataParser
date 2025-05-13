import re

def convert_brackets(file_path):
    # 读取文件内容
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # 替换全角括号为半角括号
    content = re.sub(r'（', '(', content)
    content = re.sub(r'）', ')', content)

    # 写回文件
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)

# 使用函数
convert_brackets('MedicalReportParameters.yml')