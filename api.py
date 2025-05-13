import os
import boto3
import math
import socket
import gradio as gr
from tqdm import tqdm


ozone_endpoint = ''  # 替换为你的 Ozone S3 端点
access_key = ''  # 替换为你的访问密钥
secret_key = ''  # 替换为你的秘密密钥
bucket_name = ''


class S3OPS:

    def __init__(self, access_key, secret_key, endpoint_url, bucket_name):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            region_name='cn-northwest-1'
        )
        self.bucket = bucket_name  # 存储桶名

    def _convert_size(self, size_bytes):
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"

    def _get_local_ip(self) ->str :
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("10.9.0.99", 80))
            local_ip = s.getsockname()[0]
        except Exception:
            local_ip = "127.0.0.1"
        finally:
            s.close()
        return local_ip

    @property
    def local_ip(self) -> str:
        return self._get_local_ip()

    def list_all_objects(self):
        """
        遍历桶中所有对象并返回格式化的字符串结果
        :return: 格式化后的对象列表字符串
        """
        total_size = 0
        count = 0
        output = ""

        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket)

            if 'Contents' not in response:
                return "桶中没有对象"

            objects = response['Contents']
            for obj in objects:
                total_size += obj['Size']
                count += 1
                new_line = f"找到对象: {obj['Key']}, {self._convert_size(obj['Size'])}\n"
                output += new_line
                yield output

            summary = f"\n共找到 {count} 个对象, 总使用量 {self._convert_size(total_size)}\n"
            output += summary
            yield output

        except Exception as e:
            return f"遍历桶对象失败: {str(e)}"

    def upload_files_with_tqdm(self, local_path: str, prefix: str, s3_prefix: str):
        """
        上传文件并实时返回进度（通过yield实现流式输出）
        """
        output = ''
        files = []
        # 先扫描所有匹配的文件（用于计算总进度）
        for root, _, filenames in os.walk(local_path):
            for filename in filenames:
                if filename.startswith(prefix):
                    files.append(os.path.join(root, filename))

        # 使用tqdm显示进度
        with tqdm(total=len(files)) as pbar:
            for file in files:
                local_file = file
                relative_path = os.path.relpath(local_file, local_path)
                s3_file = os.path.join(s3_prefix, relative_path).replace("\\", "/")

                try:
                    # 上传文件
                    self.s3_client.upload_file(local_file, self.bucket, s3_file)
                    newline = f"✅ 上传成功: {local_file} -> s3://{self.bucket}/{s3_file}\n"
                    output += newline
                    yield newline
                except Exception as e:
                    yield f"❌ 上传失败 {local_file}: {str(e)}\n"

                pbar.update(1)  # 更新进度条
                newline = f"📊 进度: {pbar.n}/{pbar.total} ({pbar.n / pbar.total:.0%})\n"  # 实时进度
                output += newline
                yield output

    def upload_files_with_prefix(self, local_path: str, prefix: str, s3_prefix: str):
        """
        上传符合前缀条件的文件到S3
        :param local_path: 本地目录路径
        :param s3_prefix: S3前缀
        :param prefix: 要匹配的文件名前缀(如'ops-ad')
        """
        output = ""
        for root, dirs, files in os.walk(local_path):
            for filename in files:
                if filename.startswith(prefix):
                    local_file = os.path.join(root, filename)
                    relative_path = os.path.relpath(local_file, local_path)
                    s3_file = os.path.join(s3_prefix, relative_path)
                    try:
                        self.s3_client.upload_file(local_file, self.bucket, s3_file)
                        new_line = f"上传成功: {local_file} -> s3://{self.bucket}/{s3_file}\n"
                        output += new_line
                        yield output
                    except Exception as e:
                        yield f"上传失败 {local_file}: {str(e)}"

    def delete_objects_by_prefix(self, prefix: str, s3_prefix):
        """
        删除指定前缀的所有S3对象

        :param bucket_name: 桶名称
        :param prefix: 要删除的对象前缀(如'logs/')
        """
        output = ''
        try:
            prefix = f'{s3_prefix}/' + prefix
            # 列出指定前缀的所有对象
            objects_to_delete = []
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)

            if 'Contents' not in response:
                yield f"没有找到前缀为 '{prefix}' 的对象"
                return

            # 收集要删除的对象
            for obj in response['Contents']:
                objects_to_delete.append({'Key': obj['Key']})
                newline = f"准备删除: {obj['Key']}\n"
                output += newline
                yield output

            # 执行批量删除(最多1000个对象)
            delete_response = self.s3_client.delete_objects(
                Bucket=self.bucket,
                Delete={'Objects': objects_to_delete}
            )

            newline = f"成功删除 {len(objects_to_delete)} 个对象\n"
            output += newline
            yield output

            # 检查是否有删除失败的对象
            if 'Errors' in delete_response:
                for error in delete_response['Errors']:
                    yield f"删除失败: {error['Key']} - {error['Message']}"

        except Exception as e:
            yield f"删除操作失败: {str(e)}\n"

    def download_with_prefix(self, prefix: str, local_dir: str, s3_prefix):
        """
        下载指定前缀的所有S3对象到本地目录
        :param prefix: 要下载的对象前缀(如'logs/2023-')
        :param local_dir: 本地目标目录
        """
        output = ''
        prefix = f'{s3_prefix}/{prefix}'
        try:
            # 确保本地目录存在
            os.makedirs(local_dir, exist_ok=True)

            # 列出指定前缀的所有对象
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)

            if 'Contents' not in response:
                yield f"没有找到前缀为 '{prefix}' 的对象"

            new_lines = f"开始下载 {len(response['Contents'])} 个对象到 {local_dir}\n"
            output += new_lines
            yield output

            for obj in response['Contents']:
                obj_key = obj['Key']

                # 提取文件名(去除前缀)
                relative_path = obj_key.split('/')[1]
                # 构建本地完整路径
                local_path = os.path.join(local_dir, relative_path)
                # 确保目标目录存在
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                try:
                    self.s3_client.download_file(self.bucket, obj_key, local_path)
                    new_lines = f"下载成功: {obj_key} -> {local_path}\n"
                    output += new_lines
                    yield output
                except Exception as e:
                    new_lines = f"下载失败 {obj_key}: {str(e)}\n"
                    output += new_lines
                    yield output
        except Exception as e:
            print(f"下载操作失败: {str(e)}")


s3_helper = S3OPS(access_key, secret_key, ozone_endpoint, bucket_name)

with gr.Blocks(title="对象存储") as demo:
    gr.Markdown(f"{s3_helper.local_ip}机器")

    with gr.Tab("桶文件列表"):
        list_btn = gr.Button("列出所有对象")
        list_output = gr.Textbox(label="对象列表", lines=40, autoscroll=True)

    with gr.Tab("上传对象"):
        with gr.Row():
            local_path = gr.Textbox(label="本地路径")
            file_prefix = gr.Textbox(label="文件前缀(空值默认路径下所有文件)")
            s3_prefix = gr.Textbox(label="S3 前缀")
        upload_btn = gr.Button("上传文件至S3桶中")
        upload_output = gr.Textbox(label="上传结果", lines=40, autoscroll=True)

    with gr.Tab("删除对象"):
        with gr.Row():
            remove_prefix = gr.Textbox(label="删除的文件前缀")
            remove_s3_prefix = gr.Textbox(label="S3前缀")
        remove_btn = gr.Button("删除对象")
        remove_output = gr.Textbox(label="删除结果", lines=40, autoscroll=True)

    with gr.Tab("下载对象"):
        with gr.Row():
            download_prefix = gr.Textbox(label="下载的文件前缀")
            download_s3_prefix = gr.Textbox(label="下载的S3前缀")
            download_dir = gr.Textbox(label="下载到的本地目录")
        download_btn = gr.Button("下载对象")
        download_output = gr.Textbox(label="下载结果", lines=40, autoscroll=True)

    list_btn.click(
        s3_helper.list_all_objects,
        inputs=[],
        outputs=list_output
    )

    upload_btn.click(
        s3_helper.upload_files_with_tqdm,
        inputs=[local_path, file_prefix, s3_prefix],
        outputs=upload_output
    )

    remove_btn.click(
        s3_helper.delete_objects_by_prefix,
        inputs=[remove_prefix, remove_s3_prefix],
        outputs=remove_output,
    )

    download_btn.click(
        s3_helper.download_with_prefix,
        inputs=[download_prefix, download_dir, download_s3_prefix],
        outputs=download_output
    )

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0")

