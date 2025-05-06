## 项目简介
本项目是一个基于 Python 的通用对象存储接口可视化工具。借助 Gradio 构建的可视化界面，用户能够无需编写脚本，即可快速对对象存储中的对象执行上传、下载、删除和查看等操作。此工具采用 `boto3` 库与对象存储服务进行交互，支持多种对象存储系统。

## 功能特性
- **可视化操作**：通过直观的图形界面，轻松实现对象的上传、下载、删除和查看。
- **通用接口**：使用 `boto3` 库，可适配多种对象存储服务。
- **便捷高效**：无需编写脚本，降低操作门槛，提高工作效率。

安装依赖库
```bash
pip install -r requirements.txt
```
其中 `requirements.txt` 文件内容如下：
```plaintext
boto3
gradio
```

## 使用方法
### 1. 配置对象存储信息
在脚本中配置你的对象存储服务的访问密钥、存储桶名称等信息。例如：
```python
import boto3

# 配置对象存储信息
s3 = boto3.client('s3',
                  aws_access_key_id='YOUR_ACCESS_KEY',
                  aws_secret_access_key='YOUR_SECRET_KEY',
                  endpoint_url='YOUR_ENDPOINT_URL')

bucket_name = 'YOUR_BUCKET_NAME'
```

### 2. 运行脚本
```bash
python your_script_name.py
```

### 3. 访问可视化界面
打开浏览器，访问 `http://127.0.0.1:7860`，即可看到可视化操作界面。

### 4. 操作对象
- **上传**：选择要上传的文件，点击上传按钮。
- **下载**：输入要下载的对象名称，点击下载按钮。
- **删除**：输入要删除的对象名称，点击删除按钮。
- **查看**：输入要查看的对象名称，点击查看按钮。

## 依赖库说明
- **boto3**：用于与对象存储服务进行交互的 Python 库，支持多种云服务提供商的对象存储。
- **gradio**：用于快速构建交互式 Web 界面的 Python 库，使对象存储操作更加直观和便捷。

## 贡献与反馈
如果你发现任何问题或有改进建议，欢迎提交 Issue 或 Pull Request。

## 许可证
本项目采用 [MIT 许可证](LICENSE)。

请将上述内容中的 `https://github.com/yourusername/your-repo.git`、`YOUR_ACCESS_KEY`、`YOUR_SECRET_KEY`、`YOUR_ENDPOINT_URL`、`YOUR_BUCKET_NAME` 和 `your_script_name.py` 替换为实际的项目链接、对象存储信息和脚本文件名。 
