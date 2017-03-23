from msgpack import packb, unpackb
from sqlalchemy.types import LargeBinary
from sqlalchemy.sql.type_api import TypeDecorator


class MsgpackType(TypeDecorator):
    impl = LargeBinary

    def bind_processor(self, dialect):
        impl_processor = self.impl.bind_processor(dialect)
        if impl_processor:
            def process(value):
                value = packb(value, use_bin_type=True)
                return impl_processor(value)
        else:
            def process(value):
                return packb(value, use_bin_type=True)
        return process

    def result_processor(self, dialect, coltype):
        impl_processor = self.impl.result_processor(dialect, coltype)
        if impl_processor:
            def process(value):
                value = impl_processor(value)
                return unpackb(value, encoding='utf-8')
        else:
            def process(value):
                return unpackb(value, encoding='utf-8')
        return process
