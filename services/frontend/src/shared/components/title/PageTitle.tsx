import {
    useNavigate,
    useLocation,
    useParams,
    useMatches
} from 'react-router-dom';

import { Button, Typography } from 'antd';

type PageTitleProps = {
    title: string;
    subTitle?: string | React.ReactElement;
    goBack?: boolean;
};

export default function PageTitle({
    title,
    goBack = true,
    subTitle
}: PageTitleProps) {
    const navigate = useNavigate();
    const location = useLocation();

    const handleGoBack = () => {
        navigate(location?.state?.from || -1);
    };

    return (
        <div className="mb-8">
            {goBack && (
                <Button className="mb-6" onClick={handleGoBack}>
                    Back
                </Button>
            )}
            <Typography.Title>{title}</Typography.Title>
            {subTitle && <>{subTitle}</>}
        </div>
    );
}
