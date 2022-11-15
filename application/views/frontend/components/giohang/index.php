<div class="row content-cart">
	<div class="container">
    <div id="cart-content">
        <?php if($this->session->has_userdata('Cart')):
                $cart = unserialize($this->session->userdata('Cart'));
                ?>
                <form action="" method="post" id="cartformpage">
                    <div class="cart-index">
                    <h2>Chi tiết giỏ hàng</h2>
                        <div class="tbody text-center">
                            <div class="col-xs-12 col-12 col-sm-12 col-md-8 col-lg-8">
                                <table class="table table-list-product">
                                    <thead>
                                        <tr style="background: #f3f3f3;">
                                            <th>Hình ảnh</th>
                                            <th>Tên sản phẩm</th>
                                            <th class="text-center">Đơn giá</th>
                                            <th class="text-center">Số lượng</th>
                                            <th class="text-center">Thành tiền</th>
                                            <th class="text-center">Xóa</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <?php foreach ($cart->sanPham as $value) :
                                    
                                            ?>
                                            <tr>
                                                <td class="img-product-cart">
                                                    <a href="<?php echo $value['info']['alias'] ?>">
                                                        <img src="public/images/products/<?php echo $value['info']['image'] ?>" alt="<?php echo $value['info']['name'] ?>">
                                                    </a>
                                                </td>
                                                <td>
                                                    <a href="<?php echo $value['info']['alias'] ?>" class="pull-left"><?php echo $value['info']['name'] ?></a>
                                                </td>
                                                <td>
                                                    <span class="amount">
                                                        <?php 
                                                        echo (number_format($value['info']['price'])).' VNĐ';
                                                        ?>
                                                    </span>
                                                </td>
                                                <td>
                                                    <div class="quantity clearfix">
                                                        <input name="quantity" id="<?php echo $value['info']['id'] ?>" class="form-control input-change-quanlity" type="number" value="<?php echo $value['quanlity']
                                                        ?>" min="1" max="1000" data-id="<?php echo $value['info']['id'] ?>" >
                                                    </div>
                                                </td>
                                                <td>
                                                    <span class="amount">
                                                    <?php 
                                                        echo (number_format($value['price'])).' VNĐ';
                                                        ?>
                                                    </span>
                                                </td>
                                                <td>
                                                    <a class="remove-cart" title="Xóa" data-id="<?php echo $value['info']['id']; ?>" ><i class="fas fa-trash-alt"></i></a>
                                                </td>
                                            </tr>
                                        <?php endforeach; ?>
                                    </tbody>
                                </table>
                                <button class="btn" onclick="window.location.href='san-pham'"> <a href="<?php echo base_url() ?>san-pham">Tiếp tục mua hàng</a></button>
                            </div>
        
                            <div class="col-xs-12 col-sm-12 col-md-4">
                                <div class="clearfix btn-submit" style="padding-left: 10px;margin-top: 20px;">
                                    <table class="table total-price" style="border: 1px solid #ececec;">
                                        <tbody>
                                            <tr style="background: #f4f4f4;">
                                                <td>Tổng tiền</td>
                                                <td><strong><?php echo (number_format($cart->tongGia)).' VNĐ'; ?></strong></td>
                                            </tr>
                                            <tr>
                                                <td colspan="2"><h5>Mua hàng trực tiếp tại cửa hàng giảm giá 5%</h5></td>
                                            </tr>
                                            <tr>
                                                <td colspan="2"><h5>Nếu đặt online Bạn hãy đồng ý với điều khoản sử dụng & hướng dẫn hoàn trả.</h5></td>
                                            </tr>
                                            
                                            <tr>

                                                <td colspan="2">
                                                    <button type="button" onclick="window.location.href='info-order'" class="btn-next-checkout">Đặt hàng</button>
                                                </td>
                                            </tr>
                                        </tbody>
                                    </table>

                                </div>
                            </div>
                        </div>
                    </div>

                </form>
                <?php else: ?>
                    <div class="cart-info">
                        Chưa có sản phẩm nào trong giỏ hàng !
                        <br>	
                        <button class="btn" onclick="window.location.href='san-pham'"> Tiếp tục mua hàng</button>
                    </div>

                <?php endif;?>
        </div>
	</div>
</div>
	<script>

        $('.remove-cart').on('click', function(e) { 
            const id = $(this).attr('data-id');
            const strurl="<?php echo base_url();?>"+`/gio-hang/xoa-san-pham/${id}`;
            $.ajax({
				url: strurl,
				type: 'GET',
				dataType: 'HTML',
				success: function(data) {
                    if(!data?.success) {
                        $('#cart-content').html(data);
                        updateHeader();
                    } else {
                        alert('Loi xoa san pham!');
                    }
				}
			});



        });

        $('.input-change-quanlity').on('change', function(e) {
            const id = $(this).attr('data-id');
            const quanlity = $(this).val();

            if (quanlity < 0) {
                alert('So luong thay doi loi!');
            } else {
                const strurl="<?php echo base_url();?>"+`/gio-hang/cap-nhat-san-pham/${id}/${quanlity}`;
                $.ajax({
				url: strurl,
				type: 'GET',
				dataType: 'HTML',
				success: function(data) {
                    if(!data?.success) {
                        $('#cart-content').html(data);
                        updateHeader();
                    } else if (!data.success) {
                        alert(data.error);
                    }
				}
			});
            }
        });

        // function onChangeSL(id){
		// 	var sl = document.getElementById(id).value;
		// 	var strurl="<?php echo base_url();?>"+'/sanpham/update';
		// 	jQuery.ajax({
		// 		url: strurl,
		// 		type: 'POST',
		// 		dataType: 'json',
		// 		data: {id: id, sl:sl},
		// 		success: function(data) {
		// 			document.location.reload(true);
		// 		}
		// 	});
		function onRemoveProduct(id){
		// }
			var strurl="<?php echo base_url();?>"+`/gio-hang/xoa-san-pham/${id}`;
			$.ajax({
				url: strurl,
				type: 'GET',
				dataType: 'HTML',
				success: function(data) {
					alert('Xóa sản phẩm thành công !!');
                    updateHeader();
				}
			});
        }
 	</script>