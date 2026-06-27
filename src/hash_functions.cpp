#include "hash_functions.hpp"

namespace duckdb {

void sha256(EncryptionUtil &encryption_util, const char *in, size_t in_len, hash_bytes &out) {
	D_ASSERT(CryptoHash::GetDigestSize(CryptoHashFunction::SHA256) == sizeof(hash_bytes));
	encryption_util.Hash(CryptoHashFunction::SHA256, const_data_ptr_cast(in), in_len, out);
}

void hmac256(EncryptionUtil &encryption_util, const std::string &message, const char *secret, size_t secret_len,
             hash_bytes &out) {
	D_ASSERT(CryptoHash::GetDigestSize(CryptoHashFunction::SHA256) == sizeof(hash_bytes));
	encryption_util.Hmac(CryptoHashFunction::SHA256, const_data_ptr_cast(secret), secret_len,
	                     const_data_ptr_cast(message.data()), message.size(), out);
}

void hmac256(EncryptionUtil &encryption_util, std::string message, hash_bytes secret, hash_bytes &out) {
	hmac256(encryption_util, message, reinterpret_cast<const char *>(secret), sizeof(hash_bytes), out);
}

void hex256(hash_bytes &in, hash_str &out) {
	CryptoHash::ToHex(in, sizeof(hash_bytes), reinterpret_cast<char *>(out));
}

} // namespace duckdb
