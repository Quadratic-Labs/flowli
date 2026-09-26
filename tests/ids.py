"""Fixed execution ids for tests. Version-less UUIDs: the engine never inspects the version."""

from uuid import UUID

E_ABC = UUID(int=0xABC)
E_AAA = UUID(int=0xAAA)
E_BBB = UUID(int=0xBBB)
E_DEF = UUID(int=0xDEF)
E_ZZZ = UUID(int=0x222)
E_NOPE = UUID(int=0x0DEAD)
E_PAR = UUID(int=0x9A4)
