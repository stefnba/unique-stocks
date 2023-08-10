import { useNavigate, useLocation } from 'react-router-dom';

import { Button, Typography } from 'antd';

type PageTitleProps = {
    title: string;
    goBack?: boolean;
};

export default function PageTitle({ title, goBack = true }: PageTitleProps) {
    const navigate = useNavigate();
    const location = useLocation();

    const handleGoBack = () => {
        navigate(location?.state?.from || -1);
    };

    return (
        <div className="mb-8">
            <Typography.Title>{title}</Typography.Title>
            {goBack && <Button onClick={handleGoBack}>Back</Button>}
        </div>
    );
}
