package service

import (
	"context"

	v1 "suika/api/account/v1"
	"suika/internal/biz"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AccountService struct {
	v1.UnimplementedAccountServiceServer

	uc *biz.AccountUsecase
}

func NewAccountService(uc *biz.AccountUsecase) *AccountService {
	return &AccountService{uc: uc}
}

func (s *AccountService) CreateQRLogin(ctx context.Context, req *v1.CreateQRLoginRequest) (*v1.CreateQRLoginResponse, error) {
	session, err := s.uc.CreateQRLogin(ctx)
	if err != nil {
		return nil, err
	}
	return toCreateQRLoginDTO(session), nil
}

func (s *AccountService) PollQRLogin(ctx context.Context, req *v1.PollQRLoginRequest) (*v1.PollQRLoginResponse, error) {
	poll, err := s.uc.PollQRLogin(ctx, req.GetQrcodeKey())
	if err != nil {
		return nil, err
	}
	return &v1.PollQRLoginResponse{Status: toQRLoginStatusDTO(poll.Status)}, nil
}

func (s *AccountService) GetAccountStatus(ctx context.Context, req *v1.GetAccountStatusRequest) (*v1.GetAccountStatusResponse, error) {
	info, err := s.uc.AccountStatus(ctx)
	if err != nil {
		return nil, err
	}
	return &v1.GetAccountStatusResponse{Account: toAccountInfoDTO(info)}, nil
}

func (s *AccountService) Logout(ctx context.Context, req *v1.LogoutRequest) (*v1.LogoutResponse, error) {
	if err := s.uc.Logout(ctx); err != nil {
		return nil, err
	}
	return &v1.LogoutResponse{Empty: &emptypb.Empty{}}, nil
}

func toCreateQRLoginDTO(session *biz.QRLoginSession) *v1.CreateQRLoginResponse {
	if session == nil {
		return nil
	}
	resp := &v1.CreateQRLoginResponse{
		Url:       session.URL,
		QrcodeKey: session.QRCodeKey,
	}
	if !session.ExpireTime.IsZero() {
		resp.ExpireTime = timestamppb.New(session.ExpireTime)
	}
	return resp
}

func toQRLoginStatusDTO(status biz.QRLoginStatus) v1.QRLoginStatus {
	switch status {
	case biz.QRLoginNotScanned:
		return v1.QRLoginStatus_QR_LOGIN_STATUS_NOT_SCANNED
	case biz.QRLoginScanned:
		return v1.QRLoginStatus_QR_LOGIN_STATUS_SCANNED
	case biz.QRLoginExpired:
		return v1.QRLoginStatus_QR_LOGIN_STATUS_EXPIRED
	case biz.QRLoginConfirmed:
		return v1.QRLoginStatus_QR_LOGIN_STATUS_CONFIRMED
	default:
		return v1.QRLoginStatus_QR_LOGIN_STATUS_UNSPECIFIED
	}
}

func toAccountInfoDTO(info *biz.AccountInfo) *v1.AccountInfo {
	if info == nil {
		return nil
	}
	var state v1.AccountState
	switch info.State {
	case biz.AccountLoggedIn:
		state = v1.AccountState_ACCOUNT_STATE_LOGGED_IN
	case biz.AccountExpired:
		state = v1.AccountState_ACCOUNT_STATE_EXPIRED
	default:
		state = v1.AccountState_ACCOUNT_STATE_LOGGED_OUT
	}
	return &v1.AccountInfo{
		State: state,
		Uname: info.UName,
		Mid:   info.Mid,
	}
}
