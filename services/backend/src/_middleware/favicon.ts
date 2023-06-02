export default function ignoreFaviconMiddleware(req, res, next) {
    if (req.url === '/favicon.ico') {
        res.status(204).end();
        return;
    }
    next();
}
