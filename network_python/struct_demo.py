import struct

out = []
out.append("=" * 52)
out.append("例子1：struct.pack('!I', 5) 产出什么")
out.append("=" * 52)
out.append("")
b = struct.pack("!I", 5)
out.append("数字 5 打包后得到 %d 个字节" % len(b))
out.append("")
out.append("  逐字节(十进制):   %s" % list(b))
out.append("  逐字节(十六进制): %s" % " ".join("%02X" % x for x in b))
out.append("  合起来 = 0x00000005 = 5")
out.append("")
out.append("=" * 52)
out.append("例子2：内容长度不同 -> 前缀数字不同")
out.append("=" * 52)
out.append("")
for text in [b"MSG-1", b"hi", b"hello!!"]:
    prefix = struct.pack("!I", len(text))
    out.append("内容 %-10r 长度=%d" % (text, len(text)))
    out.append("   前缀(十六进制): %s" % " ".join("%02X" % x for x in prefix))
    out.append("   完整发出: %s | %s" % (" ".join("%02X" % x for x in prefix), text.decode()))
    out.append("")

open("struct_demo.txt", "w", encoding="utf-8").write("\n".join(out))
print("ok")
