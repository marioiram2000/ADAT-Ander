class Criptografo:
    """Clase para encriptar"""
    def encriptar(txt):
        encrypted = ""
        for c in txt:
            encrypted += ord(c);
        return encrypted

    def desencriptar(txt):
        decrypted = ""
        for c in txt:
            decrypted += chr(c);
        return decrypted