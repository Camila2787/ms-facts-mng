/* React core */
import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
/* UI core */
import { Button, Tab, Tabs, TextField, Icon, Typography, Switch, FormControlLabel } from '@material-ui/core';
import { FuseAnimate, FusePageCarded, FuseLoading } from '@fuse';
import { useForm } from '@fuse/hooks';
/* GraphQL Client hooks */
import { useSubscription, useLazyQuery, useMutation } from "@apollo/react-hooks";
/* Redux */
import { useDispatch, useSelector } from 'react-redux';
import withReducer from 'app/store/withReducer';
import * as AppActions from 'app/store/actions';
import * as Actions from '../store/actions';
import reducer from '../store/reducers';
/* Tools */
import _ from '@lodash';
import { Formik } from 'formik';
import * as Yup from "yup";
import { MDText } from 'i18n-react';
import i18n from "../i18n";
/* Support pages */
import Error404Page from 'app/main/pages/Error404Page';
import Error500Page from 'app/main/pages/Error500Page';
/* GQL queries/mutation to use */
import {
    onFactsMngSharkAttackModified,
    FactsMngSharkAttack,
    FactsMngCreateSharkAttack,
    FactsMngUpdateSharkAttack
} from "../gql/SharkAttack";
import Metadata from './tabs/Metadata';
import { BasicInfo, basicInfoFormValidationsGenerator } from './tabs/BasicInfo';


/**
 * Default Aggregate data when creating 
 */
const defaultData = {
    name: '',
    description: '',
    active: true,
};

function SharkAttack(props) {
    //Redux dispatcher
    const dispatch = useDispatch();

    // current logged user
    const loggedUser = useSelector(({ auth }) => auth.user);

    // SharkAttack STATE and CRUD ops
    const [sharkAttack, setSharkAttack] = useState();
    const gqlSharkAttack = FactsMngSharkAttack({ id: props.match.params.sharkAttackId });
    const [readSharkAttack, readSharkAttackResult] = useLazyQuery(gqlSharkAttack.query, { fetchPolicy: gqlSharkAttack.fetchPolicy })
    const [createSharkAttack, createSharkAttackResult] = useMutation(FactsMngCreateSharkAttack({}).mutation);
    const [updateSharkAttack, updateSharkAttackResult] = useMutation(FactsMngUpdateSharkAttack({}).mutation);
    const onSharkAttackModifiedResult = useSubscription(...onFactsMngSharkAttackModified({ id: props.match.params.sharkAttackId }));

    //UI controls states
    const [tabValue, setTabValue] = useState(0);
    const { form, handleChange: formHandleChange, setForm } = useForm(null);
    const [errors, setErrors] = useState([]);

    //Translation services
    let T = new MDText(i18n.get(loggedUser.locale));

    /*
    *  ====== USE_EFFECT SECTION ========
    */

    /*
        Prepares the FORM:
            - if is NEW then use default data
            - if is old SharkAttack then loads the data
        Reads (from the server) a SharkAttack when:
            - having a valid props.match.params (aka ID)
            - having or changing the selected Organization ID
    */
    useEffect(() => {
        function updateSharkAttackState() {
            const params = props.match.params;
            const { sharkAttackId } = params;
            if (sharkAttackId !== 'new') {
                if (loggedUser.selectedOrganization && loggedUser.selectedOrganization.id !== "") {
                    readSharkAttack({ variables: { organizationId: loggedUser.selectedOrganization.id, id: sharkAttackId } });
                }
            } else if (loggedUser.selectedOrganization && loggedUser.selectedOrganization.id) {
                setSharkAttack({ ...defaultData, organizationId: loggedUser.selectedOrganization.id })
                dispatch(Actions.setSharkAttacksPage(0));
            }
        }
        updateSharkAttackState();
    }, [dispatch, props.match.params, loggedUser.selectedOrganization]);


    //Refresh SharkAttack state when the lazy query (READ) resolves
    useEffect(() => {
        if (readSharkAttackResult.data)
            setSharkAttack(readSharkAttackResult.data.FactsMngSharkAttack)
    }, [readSharkAttackResult])
    //Refresh SharkAttack state when the CREATE mutation resolves
    useEffect(() => {
        if (createSharkAttackResult.data && createSharkAttackResult.data.FactsMngCreateSharkAttack) {
            setSharkAttack(createSharkAttackResult.data.FactsMngCreateSharkAttack)
            props.history.push('/shark-attack-mng/shark-attacks/' + createSharkAttackResult.data.FactsMngCreateSharkAttack.id + '/');
            dispatch(AppActions.showMessage({ message: T.translate("shark_attack.create_success"), variant: 'success' }));
        }

    }, [createSharkAttackResult])
    //Refresh SharkAttack state when the UPDATE mutation resolves
    useEffect(() => {
        if (updateSharkAttackResult.data) {
            setSharkAttack(updateSharkAttackResult.data.FactsMngUpdateSharkAttack);
        }
    }, [updateSharkAttackResult])
    //Refresh SharkAttack state when GQL subscription notifies a change
    useEffect(() => {
        if (onSharkAttackModifiedResult.data) {
            setForm(onSharkAttackModifiedResult.data.FactsMngSharkAttackModified);
            dispatch(AppActions.showMessage({ message: T.translate("shark_attack.update_success"), variant: 'success' }));
        }
    }, [onSharkAttackModifiedResult.data]);


    // Keep the sync between the SharkAttack state and the form state
    useEffect(() => {
        if ((sharkAttack && !form) || (sharkAttack && form && sharkAttack.id !== form.id)) {
            setForm(sharkAttack);
        }
    }, [form, sharkAttack, setForm]);

    // DISPLAYS floating message for CRUD errors
    useEffect(() => {
        const error = createSharkAttackResult.error || updateSharkAttackResult.error;
        if (error) {
            const { graphQLErrors, networkError, message } = error;
            const errMessage = networkError
                ? JSON.stringify(networkError)
                : graphQLErrors.length === 0
                    ? message
                    : graphQLErrors[0].message.name
            dispatch(AppActions.showMessage({
                message: errMessage,
                variant: 'error'
            }));
        }
    }, [createSharkAttackResult.error, updateSharkAttackResult.error])

    /*
    *  ====== FORM HANDLERS, VALIDATORS AND LOGIC ========
    */

    /**
     * Handles Tab changes
     * @param {*} event 
     * @param {*} tabValue 
     */
    function handleChangeTab(event, tabValue) {
        setTabValue(tabValue);
    }

    /**
     * Evaluates if the logged user has enought permissions to WRITE (Create/Update/Delete) data
     */
    function canWrite() {
        return loggedUser.role.includes('SHARK_ATTACK_WRITE');
    }

    /**
     * Evals if the Save button can be submitted
     */
    function canBeSubmitted() {
        return (
            canWrite()
            && !updateSharkAttackResult.loading
            && !createSharkAttackResult.loading
            && _.isEmpty(errors)
            && !_.isEqual({ ...sharkAttack, metadata: undefined }, { ...form, metadata: undefined })
        );
    }

    /**
     * Handle the Save button action
     */
    function handleSave() {
        const { id } = form;
        if (id === undefined) {
            createSharkAttack({ variables: { input: { ...form, organizationId: loggedUser.selectedOrganization.id } } });
        } else {
            updateSharkAttack({ variables: { id, input: { ...form, id: undefined, __typename: undefined, metadata: undefined }, merge: true } });
        }
    }

    /*
    *  ====== ALTERNATIVE PAGES TO RENDER ========
    */

    // Shows an ERROR page when a really important server response fails
    const gqlError = readSharkAttackResult.error;
    if (gqlError) {
        const firstErrorMessage = gqlError.graphQLErrors[0].message;
        if (!firstErrorMessage.includes || !firstErrorMessage.includes("Cannot return null")) {
            return (<Error500Page message={T.translate("shark_attack.internal_server_error")}
                description={gqlError.graphQLErrors.map(e => `@${e.path[0]} => code ${e.message.code}: ${e.message.name}`)} />);
        }
    }

    // Shows the Loading bar if we are waiting for something mandatory
    if (!loggedUser.selectedOrganization || readSharkAttackResult.loading) {
        return (<FuseLoading />);
    }

    // Shows a NotFound page if the SharkAttack has not been found. (maybe because it belongs to other organization or the id does not exists)
    if (props.match.params.sharkAttackId !== "new" && !readSharkAttackResult.data) {
        return (<Error404Page message={T.translate("shark_attack.not_found")} />);
    }


    /*
    *  ====== FINAL PAGE TO RENDER ========
    */

    return (
        <FusePageCarded
            classes={{
                toolbar: "p-0",
                header: "min-h-72 h-72 sm:h-136 sm:min-h-136"
            }}
            header={
                form && (
                    <div className="flex flex-1 w-full items-center justify-between">

                        <div className="flex flex-col items-start max-w-full">

                            <FuseAnimate animation="transition.slideRightIn" delay={300}>
                                <Typography className="normal-case flex items-center sm:mb-12" component={Link} role="button" to="/shark-attack-mng/shark-attacks" color="inherit">
                                    <Icon className="mr-4 text-20">arrow_back</Icon>
                                    {T.translate("shark_attack.shark_attacks")}
                                </Typography>
                            </FuseAnimate>

                            <div className="flex items-center max-w-full">
                                <FuseAnimate animation="transition.expandIn" delay={300}>
                                    <Icon className="text-32 mr-0 sm:text-48 mr-12">business</Icon>
                                </FuseAnimate>

                                <div className="flex flex-col min-w-0">
                                    <FuseAnimate animation="transition.slideLeftIn" delay={300}>
                                        <Typography className="text-16 sm:text-20 truncate">
                                            {form.name ? form.name : 'New SharkAttack'}
                                        </Typography>
                                    </FuseAnimate>
                                    <FuseAnimate animation="transition.slideLeftIn" delay={300}>
                                        <Typography variant="caption">{T.translate("shark_attack.shark_attack_detail")}</Typography>
                                    </FuseAnimate>
                                </div>
                            </div>
                        </div>
                        <FuseAnimate animation="transition.slideRightIn" delay={300}>
                            <Button
                                className="whitespace-no-wrap"
                                variant="contained"
                                disabled={!canBeSubmitted()}
                                onClick={handleSave}
                            >
                                {T.translate("shark_attack.save")}
                            </Button>
                        </FuseAnimate>
                    </div>
                )
            }
            contentToolbar={
                <Tabs
                    value={tabValue}
                    onChange={handleChangeTab}
                    indicatorColor="secondary"
                    textColor="secondary"
                    variant="scrollable"
                    scrollButtons="auto"
                    classes={{ root: "w-full h-64" }}
                >
                    <Tab className="h-64 normal-case" label={T.translate("shark_attack.basic_info")} />

                    {(form && form.metadata) && (<Tab className="h-64 normal-case" label={T.translate("shark_attack.metadata_tab")} />)}
                </Tabs>
            }
            content={
                form && (
                    <div className="p-16 sm:p-24 max-w-2xl">

                        <Formik
                            initialValues={{ ...form }}
                            enableReinitialize
                            onSubmit={handleSave}
                            validationSchema={Yup.object().shape({
                                ...basicInfoFormValidationsGenerator(T)
                            })}

                        >

                            {(props) => {
                                const {
                                    values,
                                    touched,
                                    errors,
                                    setFieldTouched,
                                    handleChange,
                                    handleSubmit
                                } = props;

                                setErrors(errors);
                                const onChange = (fieldName) => (event) => {
                                    event.persist();
                                    setFieldTouched(fieldName);
                                    handleChange(event);
                                    formHandleChange(event);
                                };

                                return (
                                    <form noValidate onSubmit={handleSubmit}>
                                        {tabValue === 0 && <BasicInfo dataSource={values} {...{ T, onChange, canWrite, errors, touched }} />}
                                        {tabValue === 1 && <Metadata dataSource={values} T={T} />}
                                    </form>
                                );
                            }}
                        </Formik>



                    </div>
                )
            }
            innerScroll
        />
    )
}

export default withReducer('SharkAttackManagement', reducer)(SharkAttack);
