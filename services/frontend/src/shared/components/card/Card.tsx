import { Link } from 'react-router-dom';
import { Typography, Tag } from 'antd';
import styled from 'styled-components';

const { Text } = Typography;

const CardStyle = styled.div`
    border: 1px solid #f0f0f0;
    border-radius: 8px;
`;

type CardProps = {
    subTitle?: string;
    link?: string;
    title: string;
    tags?: string[];
};

const Card: React.FC<CardProps> = ({ title, subTitle, link, tags }) => {
    const CardContainer = () => {
        return (
            <CardStyle className="borderrounded-lg p-3 h-24 hover:shadow-md">
                {subTitle && (
                    <div className="text-xs text-slate-500">{subTitle}</div>
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

    if (link) {
        return (
            <Link to={link}>
                <CardContainer />
            </Link>
        );
    }

    return <CardContainer />;
};

export default Card;
