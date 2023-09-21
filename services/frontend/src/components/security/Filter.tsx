import FilterPane from '@sharedComponents/filter/Pane';
import { SelectFilter, SearchFilter } from '@sharedComponents/filter';

import { SecurityTypeId } from '@app/config/constants';

export default function SecurityFilter() {
    return (
        <FilterPane component="security">
            <SearchFilter field="search" />
            <SelectFilter
                showFilter={false}
                choicesApiEndpoint="security/filter/choices/security_type_id"
                label="Security Type"
                keyLabelMapping={SecurityTypeId}
                field="security_type_id"
            />
            <SelectFilter
                showFilter={true}
                choicesApiEndpoint="security/filter/choices/isin"
                label="ISIN"
                field="isin"
            />
        </FilterPane>
    );
}
