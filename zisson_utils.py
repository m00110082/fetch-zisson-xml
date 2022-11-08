# from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
# from azure.keyvault.secrets import SecretClient
import pandas as pd

# class secret(): 
#     """
#     Container for secret values fetched from our azure key vault
#     """
#     def __init__(self, secret_name, vault_url = "https://pndk-digital-credentials.vault.azure.net/"):
#         """Constructor

#         Args:
#             secret_name (str): Name of the secret in the azure key vault
#             vault_url (str, optional): vault url. Defaults to "https://pndk-digital-credentials.vault.azure.net/".
#         """
#         self._secret_name = secret_name
#         self._vault_url = vault_url
#         self._az_auth_managed = True

#     def __get_secret(self) -> 'secret':
#         """Get secret from azure key vault

#         Returns:
#             secret: the returned secret from the specified azure key vault
#         """
#         _secret = None
#         if self._az_auth_managed: 
#             try: 
#                 _secret = self._get_secret_inner(credential = ManagedIdentityCredential())
                
#             except Exception:
#                 self._az_auth_managed = False
#                 _secret = self._get_secret_inner(credential = DefaultAzureCredential())
#         else: 
#             _secret = self._get_secret_inner(credential = DefaultAzureCredential())
            
#         return _secret
    
#     def _get_secret_inner(self, credential): 
#         _secret_client = SecretClient(vault_url=self._vault_url, credential=credential)
#         _s = _secret_client.get_secret(self._secret_name)        
#         _secret_client.close()
#         return _s

#     @property
#     def value(self): 
#         """Value of the secret

#         Returns:
#             str: Value of this secret
#         """
#         _secret = self.__get_secret()
#         return _secret.value

#     @property
#     def tags(self): 
#         """Property tags of this secret. 

#         Returns:
#             dict(str): The tag collection for this secret
#         """
#         _secret = self.__get_secret()
#         return _secret.properties.tags


def filter_blank_columns(df, value_to_check='EmptyValue'):
    """
    Function to filter out incorrectly elements which are actually just containing subelemnts.
    """
    for c in df.columns:
    # print(c)
        df[c] = df[c].str.replace('\n', value_to_check)
        for i in df[c]:
            if i is None:
                break
            else:
                # print(i)
                if value_to_check in str(i):
                    # print(c, i)
                    df.drop(c, axis=1, inplace=True)
                    break
    return df     






