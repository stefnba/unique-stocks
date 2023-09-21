import { List, PaginationProps } from 'antd';

import { CardProps } from '@sharedComponents/card/Card';

interface CardListProps<T> {
    loading?: boolean;
    pagination?: PaginationProps;
    data: T[];
    card: (item: T) => React.ReactElement<CardProps>;
}

const CardList = <T,>({
    pagination,
    data,
    card,
    loading = false
}: CardListProps<T>) => {
    return (
        <List
            loading={loading}
            pagination={{
                ...pagination,
                pageSizeOptions: pagination?.pageSizeOptions || [
                    20, 40, 100, 200
                ],
                total: pagination?.total || 0,
                pageSize: pagination?.pageSize || 20,
                showTotal: (total, range) =>
                    `${range[0].toLocaleString('en-US', {
                        unitDisplay: 'short'
                    })}-${range[1].toLocaleString()} of ${total.toLocaleString(
                        'en-US',
                        {
                            unitDisplay: 'short'
                        }
                    )} items`
            }}
            grid={{
                gutter: 18,
                xs: 1,
                sm: 2,
                md: 2,
                lg: 3,
                xl: 4,
                xxl: 4
            }}
            dataSource={data}
            renderItem={(item, index) => (
                <List.Item key={index}>{card(item)}</List.Item>
            )}
        />
    );
};

export default CardList;
