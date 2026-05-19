

from prefect.blocks.core import Block
from dataclasses import dataclass, field
from pydantic import BaseModel, Field
from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials
from typing import TypedDict, TypeVar, Type, Generic
from pydantic import SecretStr


T = TypeVar('T', bound=Block)

@dataclass
class BlockEntry(Generic[T]):
    """
    A block entry is a named block that can be loaded from the Prefect block registry.
    """

    name: str
    block: T

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.name

    async def load_async(self) -> T:
        """ 
        Load the block asynchronously from the Prefect block registry.
        """
        return await self.block.aload(name=self.name)

    



    
        

def define_block(name: str, block: T, overwrite: bool = False) -> BlockEntry[T]:
    """
    Define a block and save it to the Prefect block registry.
    """

    def exists(self) -> bool:
        """
        Check if a block exists in the Prefect block registry.
        """
        try:
            block.load(name=name)
            return True
        except Exception as e:
            return False

    try:
        block.save(name=name, overwrite=overwrite)
        print(f"Block '{name}' of type '{type(block).__name__}' saved successfully")
    except Exception as e:
        print(f"Error saving block '{name}': {e}")
        raise e
    return BlockEntry(name=name, block=block)



class BlockRegistry:
    EODHD_API_KEY = define_block("eodhd-api-key", Secret(value=SecretStr("eodhd-api-key")), overwrite=True)
    AWS_CREDENTIALS = define_block("aws-credentials", AwsCredentials(aws_access_key_id="aws-access-key-id", aws_secret_access_key=SecretStr("aws-secret-access-key")), overwrite=True)


test = BlockRegistry.AWS_CREDENTIALS
print(test)

BlockRegistry.AWS_CREDENTIALS.block.load(BlockRegistry.AWS_CREDENTIALS.name)

print(BlockRegistry.AWS_CREDENTIALS.block.aws_access_key_id)
print(BlockRegistry.AWS_CREDENTIALS.block.aws_secret_access_key)


async def main():
    const = await BlockRegistry.AWS_CREDENTIALS.load_async()
    print(const)
    

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())