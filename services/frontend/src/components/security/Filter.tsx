import FilterPane from '@sharedComponents/filter/Pane';
import { SelectFilter, SearchFilter } from '@sharedComponents/filter';
import { useAppSelector } from '@redux';
import { actions } from '@features/security';
import { SecurityTypeId } from '@app/config/constants';

export default function SecurityFilter() {
    const { applied } = useAppSelector((state) => state.security.filtering);

    return (
        <FilterPane
            appliedFilters={applied}
            applyFilterAction={actions.applyFilter}
        >
            <SearchFilter />
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
