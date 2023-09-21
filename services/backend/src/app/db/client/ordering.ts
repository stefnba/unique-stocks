import type { OrderingInput } from './types.js';

import { pgFormat } from './utils.js';

export default function ordering(
    orderingInput: OrderingInput
): string | undefined {
    if (!orderingInput) return;

    const orderingList = orderingInput.map(({ logic, column }) => {
        if (logic === 'ASC') return pgFormat(' $<column:name> ', { column });
        if (logic === 'DESC')
            return pgFormat(' $<column:name> DESC ', { column });
        return '';
    });
    return orderingList.join(' AND ');
}
