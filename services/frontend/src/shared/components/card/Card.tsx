import { Link, useLocation } from 'react-router-dom';

import { Typography, Tag } from 'antd';
import styled from 'styled-components';

const { Text } = Typography;

const CardStyle = styled.div`
    border: 1px solid #f0f0f0;
    border-radius: 8px;
    background-color: #f5f5f5;
`;

export type CardProps = {
    subTitle?: string;
    link?: string;
    title: string;
    tags?: string[];
    key: string | number;
};

const CardContainer: React.FC<CardProps> = ({ title, subTitle, tags, key }) => {
    return (
        <CardStyle
            key={key}
            className="borderrounded-lg p-3 h-24 hover:shadow-md"
        >
            {subTitle && (
                <div style={{ color: '#ff7a8a' }} className="text-xs">
                    {subTitle}
                </div>
            )}
            <div>
                <Text className="text-lg" ellipsis={{ tooltip: true }}>
                    {title}
                </Text>
            </div>
            {tags && tags.map((tag) => <Tag className="mt-2">{tag}</Tag>)}
        </CardStyle>
    );
};

const Card: React.FC<CardProps> = (props) => {
    const location = useLocation();

    if (props?.link) {
        return (
            <Link
                to={{ pathname: props?.link }}
                // state={{ from: `${location.pathname}${location.search}` }}
            >
                <CardContainer {...props} />
            </Link>
        );
    }

    return <CardContainer {...props} />;
};

export default Card;
